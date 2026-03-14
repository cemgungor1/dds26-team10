"""
Correctness tests for the Kafka/Saga payment service.

Split into two test classes:

  TestSagaHTTP
    Tests the HTTP layer correctness that Saga relies on.
    No Kafka broker required — these run against the payment service directly.
    Covers credit operations, rollback simulation, atomicity under concurrency,
    and retry exhaustion.

  TestSagaKafka
    Tests the actual Saga coordination logic (_handle_command).
    Requires a running Kafka broker.
    Covers ChargePayment, RollbackPayment, idempotency, ordering, and event emission.

Run HTTP tests only:
    python -m pytest test_kafka_saga.py::TestSagaHTTP -v

Run all tests (requires Kafka):
    python -m pytest test_kafka_saga.py -v
"""

import json
import threading
import time
import unittest
import uuid

import requests

import utils as tu

PAYMENT_URL = "http://127.0.0.1:8000"

# ── Kafka helpers (only used in TestSagaKafka) ────────────────────────────────

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

KAFKA_BROKERS        = "localhost:9092"
KAFKA_COMMAND_TOPIC  = "payment-commands"
KAFKA_EVENT_TOPIC    = "payment-events"


def _send_command(command: dict) -> None:
    """Send a command dict to the payment-commands topic."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v else None,
    )
    producer.send(KAFKA_COMMAND_TOPIC, value=command, key=command.get("order_id"))
    producer.flush()
    producer.close()


def _consume_event(order_id: str, timeout: float = 10.0) -> dict | None:
    """
    Consume one event from payment-events matching the given order_id.
    Returns the event dict or None if nothing arrives within timeout seconds.
    Uses a unique consumer group so each call reads from the latest offset.
    """
    group_id = f"test-consumer-{uuid.uuid4()}"
    consumer = KafkaConsumer(
        KAFKA_EVENT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=int(timeout * 1000),
    )
    # Seek to end so we only see new messages
    consumer.poll(timeout_ms=100)
    consumer.seek_to_end()

    deadline = time.time() + timeout
    try:
        for message in consumer:
            event = message.value
            if event.get("order_id") == order_id:
                return event
            if time.time() > deadline:
                break
    finally:
        consumer.close()
    return None


def _make_command(command_type: str, user_id: str, amount: int,
                  order_id: str = None, idempotency_key: str = None,
                  attempt_id: str = "") -> dict:
    order_id = order_id or str(uuid.uuid4())
    return {
        "message_id":      str(uuid.uuid4()),
        "type":            command_type,
        "order_id":        order_id,
        "saga_id":         order_id,
        "user_id":         user_id,
        "amount":          amount,
        "idempotency_key": idempotency_key or order_id,
        "attempt_id":      attempt_id,
        "timestamp":       time.time(),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP LAYER TESTS (no Kafka required)
# ─────────────────────────────────────────────────────────────────────────────

class TestSagaHTTP(unittest.TestCase):
    """
    Tests the HTTP layer correctness that Saga relies on.
    No Kafka broker required.
    """

    # ── Credit operations ─────────────────────────────────────────

    def test_new_user_starts_with_zero_credit(self):
        user: dict = tu.create_user()
        self.assertIn('user_id', user)
        self.assertEqual(tu.find_user(user['user_id'])['credit'], 0)

    def test_add_funds_increases_credit(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        r = tu.add_credit_to_user(user_id, 100)
        self.assertTrue(tu.status_code_is_success(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_add_funds_multiple_times_accumulates(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        tu.add_credit_to_user(user_id, 30)
        tu.add_credit_to_user(user_id, 20)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_pay_deducts_credit_correctly(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        r = tu.payment_pay(user_id, 40)
        self.assertTrue(tu.status_code_is_success(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 60)

    def test_pay_exact_amount_leaves_zero(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        r = tu.payment_pay(user_id, 50)
        self.assertTrue(tu.status_code_is_success(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 0)

    def test_pay_more_than_credit_returns_failure(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        r = tu.payment_pay(user_id, 100)
        self.assertTrue(tu.status_code_is_failure(r))

    def test_credit_unchanged_after_failed_payment(self):
        """Non-atomic read-modify-write could deduct before checking balance."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)
        tu.payment_pay(user_id, 100)
        self.assertEqual(tu.find_user(user_id)['credit'], 50)

    def test_pay_with_zero_credit_returns_failure(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        r = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_failure(r))

    def test_pay_zero_amount_does_not_change_credit(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        tu.payment_pay(user_id, 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_add_zero_funds_does_not_change_credit(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        tu.add_credit_to_user(user_id, 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_large_credit_stored_and_deducted_correctly(self):
        """Integer overflow or Redis serialization issues with large values."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 10_000_000)
        self.assertEqual(tu.find_user(user_id)['credit'], 10_000_000)
        tu.payment_pay(user_id, 9_999_999)
        self.assertEqual(tu.find_user(user_id)['credit'], 1)

    def test_find_nonexistent_user_returns_failure(self):
        r = requests.get(f"{PAYMENT_URL}/payment/find_user/nonexistent-xyz")
        self.assertEqual(r.status_code, 400)

    def test_add_funds_nonexistent_user_returns_failure(self):
        r = tu.add_credit_to_user("nonexistent-xyz", 10)
        self.assertTrue(tu.status_code_is_failure(r))

    def test_pay_nonexistent_user_returns_failure(self):
        r = tu.payment_pay("nonexistent-xyz", 10)
        self.assertTrue(tu.status_code_is_failure(r))

    # ── Rollback simulation ───────────────────────────────────────

    def test_pay_then_refund_restores_credit(self):
        """Simulate saga compensation: charge happened, then refund."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        tu.payment_pay(user_id, 60)
        self.assertEqual(tu.find_user(user_id)['credit'], 40)
        tu.add_credit_to_user(user_id, 60)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

    def test_refund_after_partial_drain_restores_correct_amount(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)
        tu.payment_pay(user_id, 30)
        tu.payment_pay(user_id, 20)
        self.assertEqual(tu.find_user(user_id)['credit'], 50)
        # Refund only the first payment
        tu.add_credit_to_user(user_id, 30)
        self.assertEqual(tu.find_user(user_id)['credit'], 80)

    def test_sequential_drain_to_zero_then_fail(self):
        """Drain all credit one unit at a time, then verify next payment fails."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 10)

        for _ in range(10):
            r = tu.payment_pay(user_id, 1)
            self.assertTrue(tu.status_code_is_success(r))

        self.assertEqual(tu.find_user(user_id)['credit'], 0)
        r = tu.payment_pay(user_id, 1)
        self.assertTrue(tu.status_code_is_failure(r))
        self.assertEqual(tu.find_user(user_id)['credit'], 0)

    # ── Atomicity under concurrency (WATCH pipeline) ──────────────

    def test_concurrent_payments_never_negative(self):
        """
        5 threads each try to pay 40 from balance of 50.
        Only 1 can succeed. Credit must never go negative.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)

        results = []
        lock = threading.Lock()

        def do_pay():
            r = tu.payment_pay(user_id, 40)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=do_pay) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        credit: int = tu.find_user(user_id)['credit']
        successes = [r for r in results if tu.status_code_is_success(r)]

        self.assertGreaterEqual(credit, 0,
            f"Credit went negative: {credit}")
        self.assertEqual(len(successes), 1,
            f"Expected exactly 1 success, got {len(successes)}. Results: {results}")
        self.assertEqual(credit, 10,
            f"Expected credit=10 after one payment of 40 from 50, got {credit}")

    def test_concurrent_add_funds_all_land(self):
        """
        4 concurrent add_funds must all succeed with no lost updates.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        results = []
        lock = threading.Lock()

        def do_add():
            r = tu.add_credit_to_user(user_id, 50)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=do_add) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        successes = [r for r in results if tu.status_code_is_success(r)]
        self.assertEqual(len(successes), 4,
            f"Expected 4 successes, got {len(successes)}")
        self.assertEqual(tu.find_user(user_id)['credit'], 200,
            f"Expected credit=200 after 4x add_funds(50), got {tu.find_user(user_id)['credit']}")

    def test_concurrent_payments_total_correct(self):
        """
        100 threads each try to pay 1 from balance of 50.
        Exactly 50 must succeed and credit must end at exactly 0.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)

        results = []
        lock = threading.Lock()

        def do_pay():
            r = tu.payment_pay(user_id, 1)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=do_pay) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        credit: int = tu.find_user(user_id)['credit']
        successes = [r for r in results if tu.status_code_is_success(r)]

        self.assertEqual(credit, 0,
            f"Credit should be 0 after 50 successful payments, got {credit}")
        self.assertEqual(len(successes), 50,
            f"Expected exactly 50 successes, got {len(successes)}")

    def test_interleaved_add_and_pay_never_negative(self):
        """
        20 concurrent add_funds and 20 concurrent pay calls.
        Credit must never go negative regardless of interleaving order.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        def do_add():
            tu.add_credit_to_user(user_id, 10)

        def do_pay():
            tu.payment_pay(user_id, 15)

        threads = (
            [threading.Thread(target=do_add) for _ in range(20)] +
            [threading.Thread(target=do_pay) for _ in range(20)]
        )
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        credit: int = tu.find_user(user_id)['credit']
        self.assertGreaterEqual(credit, 0,
            f"Credit went negative under concurrent load: {credit}")

    def test_final_credit_matches_successful_payments(self):
        """
        After a contention spike, final credit must equal:
            starting_credit - (number_of_successful_payments * amount)
        Any difference indicates a double-apply or lost update.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        starting_credit = 200
        tu.add_credit_to_user(user_id, starting_credit)

        results = []
        lock = threading.Lock()

        def do_pay():
            r = tu.payment_pay(user_id, 1)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=do_pay) for _ in range(300)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        successes = [r for r in results if tu.status_code_is_success(r)]
        final_credit: int = tu.find_user(user_id)['credit']
        expected_credit = starting_credit - len(successes)

        self.assertEqual(final_credit, expected_credit,
            f"Credit mismatch: started={starting_credit}, "
            f"successes={len(successes)}, expected={expected_credit}, actual={final_credit}. "
            f"Difference of {final_credit - expected_credit} suggests double-apply or lost update.")
        self.assertGreaterEqual(final_credit, 0,
            f"Credit went negative: {final_credit}")

    # ── Retry exhaustion ──────────────────────────────────────────

    def test_exhausted_retries_return_clean_error_not_500(self):
        """
        50 concurrent payments against balance of 10.
        Failures must be clean 4xx/5xx, never 500, and must not hang.
        join(timeout=30) catches infinite retry loops.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 10)

        results = []
        lock = threading.Lock()

        def do_pay():
            r = tu.payment_pay(user_id, 1)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=do_pay) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self.assertEqual(len(results), 50,
            f"Only {len(results)}/50 threads completed — retry loop may be infinite")

        server_errors = [r for r in results if r == 500]
        self.assertEqual(len(server_errors), 0,
            f"Got 500 responses under contention: {server_errors}")

        credit: int = tu.find_user(user_id)['credit']
        self.assertGreaterEqual(credit, 0,
            f"Credit went negative: {credit}")

        successes = [r for r in results if tu.status_code_is_success(r)]
        self.assertEqual(len(successes), 10,
            f"Expected exactly 10 successes (balance=10), got {len(successes)}")

    def test_response_time_bounded_under_contention(self):
        """
        Each request must complete within 10 seconds under contention.
        Infinite retries with no backoff cause unbounded response times.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        times = []
        lock = threading.Lock()

        def do_pay():
            start = time.time()
            tu.payment_pay(user_id, 1)
            with lock:
                times.append(time.time() - start)

        threads = [threading.Thread(target=do_pay) for _ in range(40)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        self.assertEqual(len(times), 40,
            f"{40 - len(times)} threads did not complete in time")

        max_time = max(times)
        avg_time = sum(times) / len(times)

        self.assertLess(max_time, 10.0,
            f"Slowest request took {max_time:.2f}s — retry loop may lack backoff")
        self.assertLess(avg_time, 2.0,
            f"Average response time {avg_time:.2f}s too high under contention")

    def test_service_recovers_after_contention_spike(self):
        """
        After a contention spike, the service must accept new requests normally.
        Workers stuck in infinite retries would fail this test.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 5)

        def do_pay():
            tu.payment_pay(user_id, 1)

        threads = [threading.Thread(target=do_pay) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        time.sleep(1)

        user2: dict = tu.create_user()
        user_id2: str = user2['user_id']
        r = tu.add_credit_to_user(user_id2, 50)
        self.assertTrue(tu.status_code_is_success(r),
            f"Service unresponsive after contention spike: {r}")
        r = tu.payment_pay(user_id2, 50)
        self.assertTrue(tu.status_code_is_success(r),
            f"Payment failed after contention spike: {r}")
        self.assertEqual(tu.find_user(user_id2)['credit'], 0)

    def test_contention_failures_not_conflated_with_insufficient_credit(self):
        """
        KNOWN GAP: Retry exhaustion returns 400 — same as insufficient credit.
        If a user has credit remaining but got 400, those are contention failures
        misreported as business logic failures. Should be 503.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 1000)

        results = []
        lock = threading.Lock()

        def do_pay():
            r = tu.payment_pay(user_id, 1)
            with lock:
                results.append(r)

        threads = [threading.Thread(target=do_pay) for _ in range(100)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)

        server_errors = [r for r in results if r == 500]
        self.assertEqual(len(server_errors), 0,
            f"Got 500 responses: {server_errors}")

        failures = [r for r in results if not tu.status_code_is_success(r)]
        bad_requests = [r for r in failures if tu.status_code_is_failure(r)]
        if bad_requests:
            credit: int = tu.find_user(user_id)['credit']
            if credit > 0:
                self.fail(
                    f"Got {len(bad_requests)} x 400 but user still has {credit} credit "
                    f"— contention failures should return 503 not 400"
                )


# ─────────────────────────────────────────────────────────────────────────────
#  KAFKA LAYER TESTS (requires Kafka broker)
# ─────────────────────────────────────────────────────────────────────────────

@unittest.skipUnless(KAFKA_AVAILABLE, "kafka-python not installed")
class TestSagaKafka(unittest.TestCase):
    """
    Tests the actual Saga coordination logic via Kafka commands.
    Requires a running Kafka broker at localhost:9092.
    """

    # ── ChargePayment ─────────────────────────────────────────────

    def test_charge_sufficient_credit_emits_success_event(self):
        """ChargePayment with sufficient credit emits PaymentCharged(success=True)."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        cmd = _make_command("ChargePayment", user_id, 40)
        _send_command(cmd)

        event = _consume_event(cmd["order_id"])
        self.assertIsNotNone(event, "No PaymentCharged event received")
        self.assertEqual(event['type'], "PaymentCharged")
        self.assertTrue(event['success'],
            f"Expected success=True, got reason: {event.get('reason')}")
        self.assertEqual(tu.find_user(user_id)['credit'], 60)

    def test_charge_insufficient_credit_emits_failure_event(self):
        """ChargePayment with insufficient credit emits PaymentCharged(success=False)."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 10)

        cmd = _make_command("ChargePayment", user_id, 100)
        _send_command(cmd)

        event = _consume_event(cmd["order_id"])
        self.assertIsNotNone(event, "No PaymentCharged event received")
        self.assertEqual(event['type'], "PaymentCharged")
        self.assertFalse(event['success'],
            "Expected success=False for insufficient credit")
        self.assertEqual(tu.find_user(user_id)['credit'], 10,
            "Credit must not change after failed charge")

    def test_charge_nonexistent_user_emits_failure_event(self):
        """ChargePayment for nonexistent user emits PaymentCharged(success=False)."""
        cmd = _make_command("ChargePayment", "nonexistent-user-xyz", 50)
        _send_command(cmd)

        event = _consume_event(cmd["order_id"])
        self.assertIsNotNone(event, "No PaymentCharged event received")
        self.assertFalse(event['success'],
            "Expected success=False for nonexistent user")

    def test_duplicate_charge_same_message_id_ignored(self):
        """
        Duplicate ChargePayment with same message_id must be ignored.
        Credit must only be deducted once.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        cmd = _make_command("ChargePayment", user_id, 40)
        _send_command(cmd)
        time.sleep(1)  # let first command process
        _send_command(cmd)  # exact same message_id

        time.sleep(2)
        self.assertEqual(tu.find_user(user_id)['credit'], 60,
            "Credit deducted twice — message_id idempotency broken")

    def test_duplicate_charge_same_idempotency_key_not_double_charged(self):
        """
        Two ChargePayment commands with same idempotency_key but different message_id.
        Second must return PaymentCharged(success=True, reason='Already charged')
        without deducting credit again.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        cmd1 = _make_command("ChargePayment", user_id, 40,
                             order_id=order_id, idempotency_key=idem_key)
        _send_command(cmd1)
        time.sleep(1)

        cmd2 = _make_command("ChargePayment", user_id, 40,
                             order_id=order_id, idempotency_key=idem_key)
        # cmd2 has a different message_id but same idempotency_key
        _send_command(cmd2)

        event = _consume_event(order_id)
        self.assertIsNotNone(event, "No event received for second charge attempt")
        self.assertTrue(event['success'])
        self.assertIn("Already charged", event.get('reason', ''),
            f"Expected 'Already charged' reason, got: {event.get('reason')}")

        time.sleep(1)
        self.assertEqual(tu.find_user(user_id)['credit'], 60,
            "Credit deducted twice — idempotency_key deduplication broken")

    def test_charge_credit_and_charged_key_atomic(self):
        """
        After a successful ChargePayment, both the credit deduction and
        the charged_key must be set. There must be no state where credit
        is deducted but charged_key is missing (partial write).
        Verified by checking credit before the event arrives.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        cmd = _make_command("ChargePayment", user_id, 40)
        _send_command(cmd)

        event = _consume_event(cmd["order_id"])
        self.assertIsNotNone(event)
        self.assertTrue(event['success'])

        # If atomicity is broken, credit could be 60 but charged_key missing,
        # causing the next rollback to give free credit.
        # We verify by attempting a rollback and checking it restores to 100.
        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=cmd["order_id"],
                                     idempotency_key=cmd["idempotency_key"])
        _send_command(rollback_cmd)

        rollback_event = _consume_event(cmd["order_id"])
        self.assertIsNotNone(rollback_event)
        self.assertTrue(rollback_event['success'])

        time.sleep(1)
        self.assertEqual(tu.find_user(user_id)['credit'], 100,
            "Credit not fully restored after charge + rollback — atomicity broken")

    def test_charge_emits_correct_event_fields(self):
        """PaymentCharged event must contain saga_id, order_id, attempt_id, success, timestamp."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        attempt_id = "attempt-1"
        cmd = _make_command("ChargePayment", user_id, 30,
                            order_id=order_id, attempt_id=attempt_id)
        _send_command(cmd)

        event = _consume_event(order_id)
        self.assertIsNotNone(event)
        self.assertEqual(event['type'], "PaymentCharged")
        self.assertEqual(event['order_id'], order_id)
        self.assertEqual(event['saga_id'], order_id)
        self.assertEqual(event['attempt_id'], attempt_id)
        self.assertIn('success', event)
        self.assertIn('timestamp', event)
        self.assertIn('message_id', event)

    # ── RollbackPayment ───────────────────────────────────────────

    def test_rollback_charged_payment_restores_credit(self):
        """RollbackPayment after charge restores credit and emits PaymentRolledBack(success=True)."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        charge_cmd = _make_command("ChargePayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(charge_cmd)
        _consume_event(order_id)  # wait for charge to complete
        time.sleep(1)

        self.assertEqual(tu.find_user(user_id)['credit'], 60)

        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback_cmd)

        event = _consume_event(order_id)
        self.assertIsNotNone(event, "No PaymentRolledBack event received")
        self.assertEqual(event['type'], "PaymentRolledBack")
        self.assertTrue(event['success'],
            f"Rollback failed: {event.get('reason')}")

        time.sleep(1)
        self.assertEqual(tu.find_user(user_id)['credit'], 100,
            "Credit not fully restored after rollback")

    def test_rollback_nonexistent_user_emits_failure_event(self):
        """RollbackPayment for nonexistent user emits PaymentRolledBack(success=False)."""
        cmd = _make_command("RollbackPayment", "nonexistent-user-xyz", 50)
        _send_command(cmd)

        event = _consume_event(cmd["order_id"])
        self.assertIsNotNone(event, "No PaymentRolledBack event received")
        self.assertFalse(event['success'],
            "Expected success=False for nonexistent user")

    def test_duplicate_rollback_same_message_id_ignored(self):
        """
        Duplicate RollbackPayment with same message_id must be ignored.
        Credit must only be refunded once.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        charge_cmd = _make_command("ChargePayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(charge_cmd)
        time.sleep(1)

        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback_cmd)
        time.sleep(1)
        _send_command(rollback_cmd)  # exact same message_id

        time.sleep(2)
        self.assertEqual(tu.find_user(user_id)['credit'], 100,
            "Credit refunded twice — message_id idempotency broken for rollback")

    def test_duplicate_rollback_same_idempotency_key_not_double_refunded(self):
        """
        Two RollbackPayment commands with same idempotency_key but different message_id.
        Second must return PaymentRolledBack(success=True, reason='Already rolled back')
        without adding credit again.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        charge_cmd = _make_command("ChargePayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(charge_cmd)
        time.sleep(1)

        rollback1 = _make_command("RollbackPayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback1)
        time.sleep(1)

        rollback2 = _make_command("RollbackPayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback2)

        event = _consume_event(order_id)
        self.assertIsNotNone(event)
        self.assertTrue(event['success'])
        self.assertIn("Already rolled back", event.get('reason', ''),
            f"Expected 'Already rolled back', got: {event.get('reason')}")

        time.sleep(1)
        self.assertEqual(tu.find_user(user_id)['credit'], 100,
            "Credit refunded twice — idempotency_key deduplication broken for rollback")

    def test_charged_key_deleted_after_rollback(self):
        """
        After a successful rollback, the charged_key must be deleted.
        A second RollbackPayment must not report 'Already charged'.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        charge_cmd = _make_command("ChargePayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(charge_cmd)
        _consume_event(order_id)
        time.sleep(1)

        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback_cmd)
        event = _consume_event(order_id)
        self.assertIsNotNone(event)
        self.assertTrue(event['success'])

        # Attempt a second charge — charged_key is gone so this should work normally
        order_id2 = str(uuid.uuid4())
        charge_cmd2 = _make_command("ChargePayment", user_id, 40,
                                    order_id=order_id2, idempotency_key=str(uuid.uuid4()))
        _send_command(charge_cmd2)
        event2 = _consume_event(order_id2)
        self.assertIsNotNone(event2)
        self.assertTrue(event2['success'],
            "Second charge failed — charged_key may not have been deleted after rollback")
        self.assertEqual(tu.find_user(user_id)['credit'], 60)

    def test_rollback_emits_correct_event_fields(self):
        """PaymentRolledBack event must contain saga_id, order_id, attempt_id, success, timestamp."""
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        attempt_id = "attempt-1"
        idem_key = str(uuid.uuid4())

        charge_cmd = _make_command("ChargePayment", user_id, 30,
                                   order_id=order_id, idempotency_key=idem_key,
                                   attempt_id=attempt_id)
        _send_command(charge_cmd)
        _consume_event(order_id)
        time.sleep(1)

        rollback_cmd = _make_command("RollbackPayment", user_id, 30,
                                     order_id=order_id, idempotency_key=idem_key,
                                     attempt_id=attempt_id)
        _send_command(rollback_cmd)

        event = _consume_event(order_id)
        self.assertIsNotNone(event)
        self.assertEqual(event['type'], "PaymentRolledBack")
        self.assertEqual(event['order_id'], order_id)
        self.assertEqual(event['saga_id'], order_id)
        self.assertEqual(event['attempt_id'], attempt_id)
        self.assertIn('success', event)
        self.assertIn('timestamp', event)
        self.assertIn('message_id', event)

    # ── Ordering and at-least-once delivery ──────────────────────

    def test_rollback_before_charge_does_not_give_free_credit(self):
        """
        KNOWN GAP: If RollbackPayment arrives before ChargePayment
        (out-of-order Kafka delivery), the rollback should be a no-op.
        If charged_key doesn't exist, _rollback_payment still adds credit —
        this test documents that gap.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        # Send rollback BEFORE charge
        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback_cmd)
        _consume_event(order_id)
        time.sleep(1)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 100,
            f"Out-of-order rollback gave free credit: credit={credit}")

    def test_charge_after_rollback_does_not_double_charge(self):
        """
        If ChargePayment arrives after RollbackPayment (out-of-order),
        the charge must not apply since the saga already decided to abort.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        # Rollback arrives first
        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=order_id, idempotency_key=idem_key)
        _send_command(rollback_cmd)
        time.sleep(1)

        # Charge arrives after rollback
        charge_cmd = _make_command("ChargePayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(charge_cmd)
        _consume_event(order_id)
        time.sleep(1)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 100,
            f"Late charge after rollback deducted credit: credit={credit}")

    def test_charge_delivered_twice_deducts_only_once(self):
        """
        At-least-once Kafka delivery means ChargePayment may arrive twice.
        Credit must only be deducted once.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        cmd = _make_command("ChargePayment", user_id, 40)

        # Deliver twice — simulate at-least-once
        _send_command(cmd)
        _send_command(cmd)

        time.sleep(3)
        self.assertEqual(tu.find_user(user_id)['credit'], 60,
            "Credit deducted twice — at-least-once delivery not handled")

    def test_rollback_delivered_twice_refunds_only_once(self):
        """
        At-least-once delivery means RollbackPayment may arrive twice.
        Credit must only be refunded once.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        idem_key = str(uuid.uuid4())

        charge_cmd = _make_command("ChargePayment", user_id, 40,
                                   order_id=order_id, idempotency_key=idem_key)
        _send_command(charge_cmd)
        time.sleep(1)

        rollback_cmd = _make_command("RollbackPayment", user_id, 40,
                                     order_id=order_id, idempotency_key=idem_key)
        # Deliver twice
        _send_command(rollback_cmd)
        _send_command(rollback_cmd)

        time.sleep(3)
        self.assertEqual(tu.find_user(user_id)['credit'], 100,
            "Credit refunded twice — at-least-once delivery not handled for rollback")

    def test_each_command_emits_exactly_one_event(self):
        """
        ChargePayment must emit exactly one PaymentCharged event.
        Multiple events for the same order_id would confuse the saga coordinator.
        """
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        order_id = str(uuid.uuid4())
        cmd = _make_command("ChargePayment", user_id, 40, order_id=order_id)

        # Collect all events for this order_id over 5 seconds
        group_id = f"test-count-{uuid.uuid4()}"
        consumer = KafkaConsumer(
            KAFKA_EVENT_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            consumer_timeout_ms=5000,
        )
        consumer.poll(timeout_ms=100)
        consumer.seek_to_end()

        _send_command(cmd)

        events = []
        try:
            for message in consumer:
                if message.value.get("order_id") == order_id:
                    events.append(message.value)
        finally:
            consumer.close()

        self.assertEqual(len(events), 1,
            f"Expected exactly 1 event for order_id={order_id}, got {len(events)}: {events}")


if __name__ == '__main__':
    unittest.main()
