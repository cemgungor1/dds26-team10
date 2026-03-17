# Benchmarks

This folder contains all test suites for the microservices system. There are three test categories, each designed to measure a different aspect of the system.

---

## Folder Structure

```
benchmarks/
├── BCT/                              # Basic Correctness Tests
│   ├── run_tests.py                  # Main entry point - runs all test modules in order
│   ├── microservices.py              # TestMicroservices - basic service correctness
│   ├── failure_resistance.py         # TestFailureResilience - service kill/restart scenarios
│   ├── 2pc.py                        # Test2PhaseCommit - 2PC atomicity tests
│   ├── coordinator.py                # TestCoordinatorFailure - order service crash/recovery
│   ├── credits.py                    # TestCredits - payment credit correctness
│   ├── rollback.py                   # TestRollback - SAGA compensation tests
│   ├── concurrent_payments.py        # TestConcurrentPayment - atomicity under concurrent load
│   ├── kafka_saga.py                 # Kafka broker + SAGA message schema tests
│   └── utils.py                      # Shared HTTP helper functions
├── BSCT/                             # Basic Stress and Consistency Tests
│   ├── consistency-test/
│   │   ├── populate.py               # Seeds DB for consistency test
│   │   ├── run_consistency_test.py   # Main entry point
│   │   ├── stress.py                 # Fires 1000 concurrent checkouts
│   │   └── verify.py                 # Checks logs and DB for inconsistencies
│   ├── stress-test/
│   │   ├── init_orders.py            # Seeds DB for stress test
│   │   └── locustfile.py             # Locust load + failure resilience scenarios
│   ├── requirements.txt              # Python dependencies
│   └── urls.json                     # Service URLs configuration
├── PT/
│   └── postman_collection.json       # Postman API test collection
└── README.md
```

---

## Prerequisites

Make sure the full system is running before executing any tests:

```bash
docker compose up --build
```

All tests assume the gateway is reachable at `http://127.0.0.1:8000`. If your setup uses a different address, update:
- `utils.py` (`ORDER_URL`, `PAYMENT_URL`, `STOCK_URL`) for BCT tests
- `BSCT/urls.json` for BSCT tests
- The `BASE_URL` variable inside Postman for PT tests

---

## BCT - Basic Correctness Tests

**Purpose:** Verifies that each microservice behaves correctly for normal operations, failure conditions, SAGA rollback, 2PC atomicity, and concurrent access. Each test class lives in its own file and is orchestrated by `run_tests.py`.

**Location:** `BCT/`

### Setup

```bash
cd BCT
pip install requests pytest kafka-python
```

### Run all tests

```bash
python run_tests.py
```

Tests run in this order: `microservices` → `credits` → `failure_resistance` → `concurrent_payments` → `coordinator` → `rollback` → `2pc` → `kafka_saga`.

### Run a single class

```bash
python -m pytest microservices.py -v
python -m pytest rollback.py -v
python -m pytest kafka_saga.py -v
# etc.
```

---

### What each file covers

#### `microservices.py` - `TestMicroservices`

Normal operation correctness across all three services.

| Test | What it checks |
|---|---|
| `test_stock` | Create item, add/subtract stock, over-subtract returns 4xx |
| `test_payment` | Create user, add credit, pay, credit deducted correctly |
| `test_order` | Full order lifecycle: rollback on out-of-stock and insufficient credit, then successful checkout after restocking and topping up |
| `test_empty_checkout` | Checking out an empty order succeeds cleanly |

---

#### `failure_resistance.py` - `TestFailureResilience`

Behaviour when services are killed mid-operation.

| Test | What it checks |
|---|---|
| `test_stock_service_down` | Checkout returns clean failure, credit untouched when stock service is down |
| `test_payment_service_down` | Stock is rolled back when payment service is down |
| `test_stock_service_recovers` | Stock service accepts new operations correctly after restart |

---

#### `2pc.py` - `Test2PhaseCommit`

Atomicity guarantees under the two-phase commit protocol.

| Test | What it checks |
|---|---|
| `test_2pc_success_case` | All resources (stock × 2, credit) committed atomically on success |
| `test_2pc_stock_insufficient_prepare_fails` | Full abort when stock is insufficient during prepare - no changes to any resource |
| `test_2pc_payment_insufficient_prepare_fails` | Full abort when credit is insufficient during prepare - stock not deducted |
| `test_2pc_atomicity_multiple_items` | 4-item order: correct stock deductions and credit charge, unused item untouched |

---

#### `coordinator.py` - `TestCoordinatorFailure`

Order service (coordinator) crash and recovery scenarios.

| Test | What it checks |
|---|---|
| `test_crash_before_commit_point` | Stock and credit unchanged after order service is killed and restarted before any commit |
| `test_system_works_after_coordinator_recovery` | Full checkout succeeds correctly after order service restarts |

---

#### `credits.py` - `TestCredits`

Payment service credit correctness covering all edge cases.

| Test | What it checks |
|---|---|
| `test_new_user_starts_with_zero_credit` | New users have 0 credit |
| `test_add_funds_increases_credit` | Credit increases correctly after add |
| `test_add_funds_multiple_times_accumulates` | Three sequential adds accumulate correctly |
| `test_pay_deducts_credit_correctly` | Payment deducts the right amount |
| `test_pay_exact_amount_leaves_zero` | Paying exact balance leaves 0, not negative |
| `test_pay_more_than_credit_returns_failure` | Overpayment returns 4xx |
| `test_credit_unchanged_after_failed_payment` | Credit untouched after failed pay |
| `test_pay_with_zero_credit_returns_failure` | Paying with no credit returns 4xx |
| `test_pay_zero_amount_does_not_change_credit` | Zero-amount pay is a no-op |
| `test_add_zero_funds_does_not_change_credit` | Zero-amount add is a no-op |
| `test_large_credit_stored_and_deducted_correctly` | Works correctly with large values (10M+) |
| `test_find_nonexistent_user_returns_failure` | Returns 400 for unknown user lookup |
| `test_add_funds_nonexistent_user_returns_failure` | Returns 4xx when adding funds to unknown user |
| `test_pay_nonexistent_user_returns_failure` | Returns 4xx when paying as unknown user |

---

#### `rollback.py` - `TestRollback`

SAGA compensating transaction correctness. Every test verifies that when a checkout fails, all previously touched resources are fully restored.

| Test | What it checks |
|---|---|
| `test_success_commits_all_resources` | Successful SAGA deducts stock and credit atomically |
| `test_rolls_back_stock_when_payment_fails` | Stock restored when payment step fails (insufficient credit) |
| `test_does_not_charge_payment_when_stock_fails` | Credit untouched when stock step fails (insufficient stock) |
| `test_rolls_back_all_items_on_payment_failure` | Every item in a 3-item order is individually compensated |
| `test_exact_credit_succeeds_second_checkout_rolls_back` | No double-charge on a second checkout after credit hits 0 |
| `test_rolls_back_stock_when_payment_service_down` | Stock compensation fires even when payment service was down |
| `test_does_not_charge_when_stock_service_down` | Credit untouched when stock service was down |
| `test_duplicate_checkout_does_not_double_charge` | Already-paid order is not charged a second time |
| `test_order_remains_unpaid_after_rollback` | Order stays unpaid after failure; retry with topped-up credit succeeds |
| `test_system_healthy_after_rollback` | Valid checkouts succeed normally after a prior rollback |

---

#### `concurrent_payments.py` - `TestConcurrentPayment`

Atomicity and correctness under concurrent load. Detects lost updates, double-applies, and infinite retry loops.

| Test | What it checks |
|---|---|
| `test_concurrent_payments_never_negative` | 5 threads paying 40 from 50 - exactly 1 succeeds, credit ends at 10, never negative |
| `test_concurrent_add_funds_all_land` | 4 concurrent adds of 50 - all succeed, credit ends at 200 with no lost updates |
| `test_concurrent_payments_total_correct` | 100 threads paying 1 from 50 - exactly 50 succeed, credit ends at 0 |
| `test_interleaved_add_and_pay_never_negative` | 20 concurrent adds and 20 concurrent pays - balance never goes negative |
| `test_final_credit_matches_successful_payments` | Final balance == starting − (successes × amount); catches double-apply and lost updates |
| `test_exhausted_retries_return_clean_error_not_500` | Retry exhaustion returns 4xx not 500; no infinite loops; all 50 threads complete |
| `test_response_time_bounded_under_contention` | Max response time < 10s, average < 2s under 40 concurrent threads |
| `test_service_recovers_after_contention_spike` | Service accepts new requests normally after a spike of 30 threads |

---

#### `kafka_saga.py` - Kafka tests

Kafka broker correctness and SAGA message schema validation. These tests talk directly to Kafka on `localhost:9092` - **no microservices need to be running**.

> **Prerequisite:** Kafka must be reachable on `localhost:9092`. Make sure `docker-compose.yml` publishes the port with the two-listener setup:
> - Internal: `kafka:29092` (used by services)
> - External: `localhost:9092` (used by these tests)

**`TestKafkaBrokerConnectivity`** - broker is reachable and operational

| Test | What it checks |
|---|---|
| `test_producer_can_connect` | KafkaProducer initialises without error |
| `test_consumer_can_connect` | KafkaConsumer initialises and subscribes to a topic |
| `test_admin_client_can_list_topics` | Admin client can retrieve topic metadata |

**`TestKafkaProduceConsume`** - message round-trip correctness

| Test | What it checks |
|---|---|
| `test_single_message_round_trip` | Value produced == value consumed |
| `test_multiple_messages_all_received` | All 10 produced messages arrive with no drops |
| `test_message_order_preserved_single_partition` | 20 messages arrive in production order within a single partition |
| `test_message_with_key_is_received_with_same_key` | Key survives the round-trip as bytes |
| `test_large_payload_round_trip` | 50 KB message arrives without truncation |

**`TestKafkaTopicManagement`** - topic lifecycle

| Test | What it checks |
|---|---|
| `test_create_topic_appears_in_listing` | Newly created topic visible in admin listing |
| `test_create_topic_with_multiple_partitions` | Topic with 3 partitions created successfully |
| `test_topic_created_implicitly_on_produce` | Auto-topic creation works on first produce |
| `test_produce_to_payment_commands_topic` | Real system topic `payment-commands` is writable |
| `test_produce_to_stock_commands_topic` | Real system topic `stock-commands` is writable |

**`TestKafkaConsumerGroups`** - offset and group behaviour

| Test | What it checks |
|---|---|
| `test_committed_offset_not_redelivered` | Second consumer in same group only sees messages after the commit point |
| `test_different_groups_both_receive_all_messages` | Two independent groups each receive the full 5-message set |
| `test_auto_offset_reset_earliest_reads_from_start` | New consumer group reads all messages produced before it joined |

**`TestKafkaConcurrentProducers`** - correctness under concurrent load

| Test | What it checks |
|---|---|
| `test_concurrent_producers_all_messages_arrive` | 10 threads × 10 messages = 100 total; none missing, none duplicated |
| `test_concurrent_producers_no_message_corruption` | All messages retain their schema under concurrent load |

**`TestKafkaSagaMessageSchema`** - SAGA command schema validation

| Test | What it checks |
|---|---|
| `test_payment_command_schema_preserved` | `ReservePayment` fields (`type`, `order_id`, `user_id`, `amount`) survive round-trip |
| `test_stock_command_schema_preserved` | `SubtractStock` fields (`type`, `order_id`, `item_id`, `quantity`) survive round-trip |
| `test_rollback_command_schema_preserved` | `RollbackPayment` fields including `reason` survive round-trip |
| `test_numeric_amount_not_coerced_to_string` | Amount arrives as `int`, not coerced to `str` |
| `test_zero_amount_command_preserved` | Zero amount is not dropped or treated as falsy |

---

## BSCT - Basic Stress and Consistency Tests

**Purpose:** Tests the system under high concurrency to verify throughput, latency, and that the database stays consistent when many requests arrive simultaneously.

**Location:** `BSCT/`

### Setup

```bash
cd BSCT
pip install -r requirements.txt
```

Confirm `urls.json` points at your gateway:

```json
{
  "ORDER_URL":   "http://127.0.0.1:8000",
  "PAYMENT_URL": "http://127.0.0.1:8000",
  "STOCK_URL":   "http://127.0.0.1:8000"
}
```

---

### Consistency Test

Populates the database with 1 item (100 stock, price 1) and 1000 users (1 credit each), then fires 1000 concurrent checkouts. Only ~10% should succeed. After the run it checks both the service responses and the actual database state for inconsistencies.

```bash
cd consistency-test
python run_consistency_test.py
```

| Metric | Meaning |
|---|---|
| Log inconsistencies | Service returned a response that didn't match what actually happened |
| Database inconsistencies | Stored state is wrong - e.g. credit deducted but stock not subtracted |
| **Target** | **0 database inconsistencies in both services** |

---

### Stress Test

Runs a high-volume Locust load test with two user types: normal checkout traffic and failure injection scenarios running simultaneously.

**Step 1 - Initialize the database:**

```bash
cd stress-test
python init_orders.py
```

Seeds the database with:

| Setting | Value |
|---|---|
| Items | 100,000 |
| Item stock | 1,000,000 each |
| Item price | 1 |
| Users | 100,000 |
| User credit | 1,000,000 each |
| Orders | 100,000 |

**Step 2 - Run Locust:**

```bash
locust -f locustfile.py --host="http://localhost:8000"
```

To select which user classes to run independently:

```bash
locust -f locustfile.py --host="http://localhost:8000" --class-picker
```

For higher load with multiple worker processes:

```bash
locust -f locustfile.py --host="http://localhost:8000" --processes 4
```

**Step 3 - Open the Locust UI at `http://localhost:8089`**

Enter the number of `MicroservicesUser`s and `FailureResilienceUser`s and click Start.

**Step 4 - What each Locust row means:**

| Row name | User class | What it tests |
|---|---|---|
| `/orders/checkout/[order_id]` | MicroservicesUser | Normal checkout throughput |
| `/orders/checkout/[stock-down]` | FailureResilienceUser | Graceful failure when stock service is killed |
| `/orders/checkout/[payment-down]` | FailureResilienceUser | Graceful failure when payment service is killed |
| `/orders/checkout/[post-stock-recovery]` | FailureResilienceUser | Correct operation after stock service restarts |
| `/orders/checkout/[post-payment-recovery]` | FailureResilienceUser | Correct operation after payment service restarts |
| `/payment/find_user/[failure-verify]` | FailureResilienceUser | Credit unchanged during stock outage |
| `/stock/find/[failure-verify]` | FailureResilienceUser | Stock rolled back during payment outage |

---

## PT - Postman Tests

**Purpose:** Manual and automated API testing for every endpoint across all three services. Tests cover happy paths, error cases, and consistency checks - for example verifying stock is rolled back after a failed checkout.

**Location:** `PT/postman_collection.json`

### Setup

1. Install [Postman](https://www.postman.com/downloads/)
2. Open Postman → click **Import**
3. Select `PT/postman_collection.json` → click **Import**
4. Click the collection **"Microservices Test Suite"** → **Variables** tab
5. Confirm `BASE_URL` is `http://127.0.0.1:8000`

### Run all tests

1. Right-click the collection → **Run collection**
2. Ensure **"Keep variable values"** is on - requests pass IDs between steps
3. Click **Run Microservices Test Suite**

### Test coverage

| Group | Endpoints tested |
|---|---|
| Stock Service | Create item, find item, add stock, subtract stock, over-subtract returns 4xx, stock unchanged after failed subtract |
| Payment Service | Create user, find user, add funds, pay, insufficient credit returns 4xx, credit unchanged after failed pay |
| Order Service | Create order, find order, add items, checkout failure with stock rollback verification, checkout success with stock and credit verification |
