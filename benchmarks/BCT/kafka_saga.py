import unittest
import subprocess
import requests
import time
import os
import threading
import json

import utils as tu

COMPOSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

PAYMENT_URL = "http://127.0.0.1:8000"

ROLLBACK_TIMEOUT = 15 # seconds to wait for async

import uuid

# Kafka Connections
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = "localhost:9092"
TIMEOUT_MS = 10_000
POLL_TIMEOUT = 15

# Helper Functions

def _unique_topic(prefix: str = "test") -> str:
    # Comes up with a unique topic name to make different topics
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def _make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
        acks="all",                  # wait for full ISR acknowledgement
        retries=3,
    )

def _make_consumer(topic: str, group: str | None = None) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=group or f"test-group-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="earliest",   # always read from the beginning
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=TIMEOUT_MS,
    )

def _create_topic(name: str, partitions: int = 1, replication: int = 1):
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        admin.create_topics([NewTopic(name, partitions, replication)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

def _drain(consumer: KafkaConsumer, max_messages: int = 100) -> list:
    # Collect messages from a consumer and return them 
    # (no. messages <= max_messages)
    messages = []
    for msg in consumer:
        messages.append(msg)
        if len(messages) >= max_messages:
            break
    return messages

class TestKafkaBrokerConnectivity(unittest.TestCase):
    # Make sure the Kafka Broker is reachable :D
    # Also tests for basic Broker operations

    def test_producer_can_connect(self):
        producer = _make_producer()
        producer.close()

    def test_consumer_can_connect(self):
        # Make sure a Kafka consumer can be made for a topic
        topic = _unique_topic("connectivity")
        _create_topic(topic)
        consumer = _make_consumer(topic)
        consumer.close()

    def test_admin_client_can_list_topics(self):
        # Test if topic lists can be created, and are the right ones
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        topics = admin.list_topics()
        admin.close()
        self.assertIsInstance(topics, list)

class TestKafkaProduceConsume(unittest.TestCase):

    def test_single_message_round_trip(self):
        # Produce a message, consume it, make sure the value is the same
        topic = _unique_topic("roundtrip")
        _create_topic(topic)

        payload = {"event": "test", "value": 42}

        producer = _make_producer()
        producer.send(topic, value=payload)
        producer.flush()
        producer.close()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=1)
        consumer.close()

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].value, payload)

    def test_multiple_messages_all_received(self):
        # Make sure the 10 Produced messages arrive without drops
        topic = _unique_topic("multi")
        _create_topic(topic)

        payloads = [{"index": i} for i in range(10)]
        producer = _make_producer()
        for p in payloads:
            producer.send(topic, value=p)
        producer.flush()
        producer.close()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=10)
        consumer.close()

        received = [m.value for m in messages]
        self.assertEqual(len(received), 10)
        for p in payloads:
            self.assertIn(p, received, f"Missing payload: {p}")

    def test_message_order_preserved_single_partition(self):
        # Make sure the order is preserved from Production to Consumption
        # In a single partition
        topic = _unique_topic("order")
        _create_topic(topic, partitions=1)

        producer = _make_producer()
        for i in range(20):
            producer.send(topic, key="same-key", value={"seq": i})
        producer.flush()
        producer.close()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=20)
        consumer.close()

        sequences = [m.value["seq"] for m in messages]
        self.assertEqual(sequences, list(range(20)),
            f"Messages out of order: {sequences}")

    def test_message_with_key_is_received_with_same_key(self):
        # Make sure the message key survives the round-trip
        topic = _unique_topic("keyed")
        _create_topic(topic)

        producer = _make_producer()
        producer.send(topic, key="my-key", value={"data": "hello"})
        producer.flush()
        producer.close()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=1)
        consumer.close()

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].key, b"my-key")

    def test_large_payload_round_trip(self):
        # Make sure a 50Kb message can be Produced and Consumed, without any loss
        topic = _unique_topic("large")
        _create_topic(topic)

        big_value = {"data": "x" * 50_000}
        producer = _make_producer()
        producer.send(topic, value=big_value)
        producer.flush()
        producer.close()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=1)
        consumer.close()

        self.assertEqual(len(messages[0].value["data"]), 50_000)

class TestKafkaTopicManagement(unittest.TestCase):

    def test_create_topic_appears_in_listing(self):
        # Checks if newly created topic is in the listing
        topic = _unique_topic("create")
        _create_topic(topic)

        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        topics = admin.list_topics()
        admin.close()

        self.assertIn(topic, topics, f"Newly created topic '{topic}' not found")

    def test_create_topic_with_multiple_partitions(self):
        # Creates a topic with 3 partitions
        topic = _unique_topic("multipart")
        _create_topic(topic, partitions=3)

        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        topics = admin.list_topics()
        admin.close()

        self.assertIn(topic, topics)

    def test_topic_created_implicitly_on_produce(self):
        # Produces a topic that is not explicitly created
        # KAFKA needs to create a topic automatically
        topic = _unique_topic("implicit")
        producer = _make_producer()
        future = producer.send(topic, value={"auto": True})
        record_metadata = future.get(timeout=10)   # raises on error
        producer.close()
        self.assertEqual(record_metadata.topic, topic)

    def test_produce_to_payment_commands_topic(self):
        producer = _make_producer()
        future = producer.send("payment-commands", value={"type": "probe"})
        future.get(timeout=10)
        producer.close()

    def test_produce_to_stock_commands_topic(self):
        producer = _make_producer()
        future = producer.send("stock-commands", value={"type": "probe"})
        future.get(timeout=10)
        producer.close()

class TestKafkaConsumerGroups(unittest.TestCase):

    def test_committed_offset_not_redelivered(self):
        # Produces 5 messages, consume and commits with consumer group A
        # Produces 3 more messages
        # Creates a second consumer in the same group
        # It must only see the 3 new messages, not the 5

        topic = _unique_topic("offset")
        _create_topic(topic)
        group = f"group-{uuid.uuid4().hex[:6]}"

        # produce 5 messages
        producer = _make_producer()
        for i in range(5):
            producer.send(topic, value={"i": i})
        producer.flush()
        producer.close()

        # first consumer reads and commits all 5
        c1 = _make_consumer(topic, group)
        first_batch = _drain(c1, max_messages=5)
        c1.commit()
        c1.close()
        self.assertEqual(len(first_batch), 5)

        # produce 3 more AFTER the commit
        producer = _make_producer()
        for i in range(5, 8):
            producer.send(topic, value={"i": i})
        producer.flush()
        producer.close()

        # second consumer in same group should only see the 3 new messages
        c2 = _make_consumer(topic, group)
        second_batch = _drain(c2, max_messages=10)
        c2.close()

        values = [m.value["i"] for m in second_batch]
        self.assertEqual(sorted(values), [5, 6, 7],
            f"Expected only new messages [5,6,7], got {values}")

    def test_different_groups_both_receive_all_messages(self):
        # Creates 2 consumers in seperate groups
        # Both should receive 5 messages independently
        topic = _unique_topic("pubsub")
        _create_topic(topic)

        payloads = [{"n": i} for i in range(5)]
        producer = _make_producer()
        for p in payloads:
            producer.send(topic, value=p)
        producer.flush()
        producer.close()

        c_a = _make_consumer(topic, f"group-a-{uuid.uuid4().hex[:4]}")
        c_b = _make_consumer(topic, f"group-b-{uuid.uuid4().hex[:4]}")

        msgs_a = _drain(c_a, max_messages=5)
        msgs_b = _drain(c_b, max_messages=5)

        c_a.close()
        c_b.close()

        self.assertEqual(len(msgs_a), 5, "Group A did not receive all messages")
        self.assertEqual(len(msgs_b), 5, "Group B did not receive all messages")

    def test_auto_offset_reset_earliest_reads_from_start(self):
        # Produces 4 messages, waits a little,
        # then creates a new consumer group
        # System must read all 4
        topic = _unique_topic("earliest")
        _create_topic(topic)

        producer = _make_producer()
        for i in range(4):
            producer.send(topic, value={"x": i})
        producer.flush()
        producer.close()

        time.sleep(20)  # let broker settle

        consumer = _make_consumer(topic)  # always uses earliest + new group
        messages = _drain(consumer, max_messages=4)
        consumer.close()

        self.assertEqual(len(messages), 4)

class TestKafkaConcurrentProducers(unittest.TestCase):

    def test_concurrent_producers_all_messages_arrive(self):
        # 10 threads send 10 messages each with a unique ID
        # None of them should go missing
        topic = _unique_topic("concurrent")
        _create_topic(topic)

        num_threads = 10
        msgs_per_thread = 10
        sent_ids = set()
        lock = threading.Lock()

        def produce(thread_id: int):
            p = _make_producer()
            for i in range(msgs_per_thread):
                uid = f"{thread_id}-{i}"
                p.send(topic, value={"id": uid})
                with lock:
                    sent_ids.add(uid)
            p.flush()
            p.close()

        threads = [threading.Thread(target=produce, args=(t,))
                   for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=num_threads * msgs_per_thread)
        consumer.close()

        received_ids = {m.value["id"] for m in messages}
        self.assertEqual(
            received_ids, sent_ids,
            f"Missing: {sent_ids - received_ids}, Extra: {received_ids - sent_ids}",
        )

    def test_concurrent_producers_no_message_corruption(self):
        # After concurrent load, make sure messages are not corrupt,
        # Def. Corrupt meaning the message can be deserialized back
        # into the python dictionary structure
        topic = _unique_topic("corruption")
        _create_topic(topic)

        def produce():
            p = _make_producer()
            for _ in range(20):
                p.send(topic, value={"type": "CheckStock", "amount": 10})
            p.flush()
            p.close()

        threads = [threading.Thread(target=produce) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=100)
        consumer.close()

        for msg in messages:
            self.assertIn("type", msg.value,   "Missing 'type' field")
            self.assertIn("amount", msg.value, "Missing 'amount' field")

class TestKafkaSagaMessageSchema(unittest.TestCase):
    PAYMENT_COMMANDS = "payment-commands"
    PAYMENT_EVENTS   = "payment-events"
    STOCK_COMMANDS   = "stock-commands"
    STOCK_EVENTS     = "stock-events"

    def _send_and_receive(self, topic: str, payload: dict) -> dict:

        # Create isolated topic
        isolated_topic = _unique_topic(f"schema-test-{topic}")
        _create_topic(isolated_topic)

        # Create consumer
        consumer = _make_consumer(isolated_topic)
        #consumer.poll(timeout_ms=100)
        #consumer.seek_to_end()

        # Produces one message to a topic, immediately consumes it back
        producer = _make_producer()
        producer.send(isolated_topic, value=payload)
        producer.flush()
        producer.close()

        #consumer = _make_consumer(topic)
        messages = _drain(consumer, max_messages=1)
        consumer.close()
        return messages[0].value

    def test_payment_command_schema_preserved(self):
        # Sends a dummy ReservePayment command,
        # asserts every field in the round trip
        cmd = {
            "type": "ReservePayment",
            "order_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "amount": 99,
        }
        received = self._send_and_receive(self.PAYMENT_COMMANDS, cmd)

        self.assertEqual(received["type"], cmd["type"])
        self.assertEqual(received["order_id"], cmd["order_id"])
        self.assertEqual(received["user_id"], cmd["user_id"])
        self.assertEqual(received["amount"], cmd["amount"])

    def test_stock_command_schema_preserved(self):
        # Sends a dummy SubtractStock command,
        # asserts every field in the round trip
        cmd = {
            "type": "SubtractStock",
            "order_id": str(uuid.uuid4()),
            "item_id": str(uuid.uuid4()),
            "quantity": 5,
        }
        received = self._send_and_receive(self.STOCK_COMMANDS, cmd)

        self.assertEqual(received["type"], cmd["type"])
        self.assertEqual(received["order_id"], cmd["order_id"])
        self.assertEqual(received["item_id"], cmd["item_id"])
        self.assertEqual(received["quantity"], cmd["quantity"])

    def test_rollback_command_schema_preserved(self):
        # Test the rollback command,
        # Assert it's fields
        cmd = {
            "type": "RollbackPayment",
            "order_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "amount": 50,
            "reason": "StockUnavailable",
        }
        received = self._send_and_receive(self.PAYMENT_COMMANDS, cmd)

        self.assertEqual(received["type"],     cmd["type"])
        self.assertEqual(received["order_id"], cmd["order_id"])
        self.assertEqual(received["reason"],   cmd["reason"])

    def test_numeric_amount_not_coerced_to_string(self):
        # Test the format of Kafka numbers, for example 3 rather than "3"
        cmd = {"type": "ReservePayment", "amount": 123, "order_id": "abc"}
        received = self._send_and_receive(self.PAYMENT_COMMANDS, cmd)
        self.assertIsInstance(received["amount"], int,
            f"Amount was coerced to {type(received['amount'])}")

    def test_zero_amount_command_preserved(self):
        # Tests commands with zero costs
        cmd = {"type": "ReservePayment", "amount": 0, "order_id": "zero-test"}
        received = self._send_and_receive(self.PAYMENT_COMMANDS, cmd)
        self.assertEqual(received["amount"], 0)
