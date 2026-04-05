import unittest
import subprocess
import requests
import time
import os
import threading

import utils as tu

COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

PAYMENT_URL = "http://127.0.0.1:8000"

ROLLBACK_TIMEOUT = 15 # seconds to wait for async

class TestConcurrentPayment(unittest.TestCase):

    def test_concurrent_payments_never_negative(self):
        # Create 5 threads to each pay 40 when credit of user is 50,
        # Only 1 must me able to pay
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        tu.add_credit_to_user(user_id, 50)

        results = []
        lock = threading.Lock()

        # Define function to pay
        def do_pay():
            r = tu.payment_pay(user_id, 40)
            with lock:
                results.append(r)

        # Create and wait for threads that run the function
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
        # Add credit to a use in parallel, make sure no transaction fails
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
        # 100 threads try to pay 1 from balance of 50!
        # 50 must succeed, and the user credit at the end must be 0
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
        # Both increasing and decreasing credit of a user with threads
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
        # 300 threads try to pay 1 from balance of 200!
        # 200 must succeed, and the user credit at the end must be 0

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

    def test_exhausted_retries_return_clean_error_not_500(self):
        # 50 threads running to make concurrent payments
        # Make sure the response codes are not 500 and responses don't hang (or have infinite loops)
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
        # Threads rrrrrunnnnningggg
        # Each request must take 10 seconds!
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
        # After maaannny threads, the system must run as usual :D
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

        time.sleep(20)

        user2: dict = tu.create_user()
        user_id2: str = user2['user_id']
        r = tu.add_credit_to_user(user_id2, 50)
        self.assertTrue(tu.status_code_is_success(r),
            f"Service unresponsive after contention spike: {r}")
        r = tu.payment_pay(user_id2, 50)
        self.assertTrue(tu.status_code_is_success(r),
            f"Payment failed after contention spike: {r}")
        self.assertEqual(tu.find_user(user_id2)['credit'], 0)


