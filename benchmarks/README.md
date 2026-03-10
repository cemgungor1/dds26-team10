# Benchmarks

This folder contains all test suites for the microservices system. There are three test categories, each designed to measure a different aspect of the system.

---

## Folder Structure

```
benchmarks/
├── BCT/                              # Basic Correctness Tests
│   ├── test_microservices.py         # Unit-style correctness tests + failure resilience tests
│   └── utils.py                      # Shared HTTP helper functions
├── BSCT/                             # Basic Stress and Consistency Tests
│   └── wdm-project-benchmark/
│       ├── consistency-test/
│       │   ├── populate.py           # Seeds DB for consistency test
│       │   ├── run_consistency_test.py  # Main entry point
│       │   ├── stress.py             # Fires 1000 concurrent checkouts
│       │   └── verify.py            # Checks logs and DB for inconsistencies
│       ├── stress-test/
│       │   ├── init_orders.py        # Seeds DB for stress test
│       │   └── locustfile.py         # Locust load + failure resilience scenarios
│       ├── requirements.txt          # Python dependencies
│       └── urls.json                 # Service URLs configuration
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
- `BSCT/wdm-project-benchmark/urls.json` for BSCT tests
- The `BASE_URL` variable inside Postman for PT tests

---

## BCT - Basic Correctness Tests

**Purpose:** Verifies that each microservice behaves correctly for normal operations and under failure conditions. Covers the full order lifecycle, rollback on failure, and resilience when services are killed and restarted.

**Location:** `BCT/`

### Setup

```bash
cd BCT
pip install requests pytest
```

### Run all tests

```bash
python -m pytest test_microservices.py -v
```

### Run only correctness tests

```bash
python -m pytest test_microservices.py::TestMicroservices -v
```

### Run only failure resilience tests

```bash
python -m pytest test_microservices.py::TestFailureResilience -v
```

### What each test covers

**`TestMicroservices`** - normal operation correctness:

| Test | What it checks |
|---|---|
| `test_stock` | Create item, add/subtract stock, over-subtract returns 400 |
| `test_payment` | Create user, add credit, pay, insufficient credit returns 400 |
| `test_order` | Full order lifecycle, rollback on out-of-stock and no credit |

**`TestFailureResilience`** - behaviour when services are killed:

| Test | What it checks |
|---|---|
| `test_stock_service_down` | Checkout returns clean failure, credit untouched when stock is down |
| `test_payment_service_down` | Stock is rolled back when payment service is down |
| `test_stock_service_recovers` | Stock service works correctly after restart |
| `test_payment_service_recovers` | Payment service works correctly after restart |
| `test_full_checkout_after_recovery` | Full checkout succeeds after both services restarted |

---

## BSCT - Basic Stress and Consistency Tests

**Purpose:** Tests the system under high concurrency to verify throughput, latency, and that the database stays consistent when many requests arrive simultaneously.

**Location:** `BSCT/wdm-project-benchmark/`

### Setup

```bash
cd BSCT/wdm-project-benchmark
pip install -r requirements.txt
```

Update `urls.json` with your gateway address:

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

```
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

To select which user classes to spawn independently, use the class picker flag:

```bash
locust -f locustfile.py --host="http://localhost:8000" --class-picker
```

For higher load with multiple worker processes:

```bash
locust -f locustfile.py --host="http://localhost:8000" --processes 4
```

**Step 3 - Open the Locust UI at `http://localhost:8089`**

Enter the number of MicroservicesUser's and the number of FailureResilienceUser's, with additional parameters to start the tests.



**Step 4 - What each Locust row means:**

| Row name | User class | What it tests |
|---|---|---|
| `/orders/checkout/[order_id]` | MicroservicesUser | Normal throughput |
| `/orders/checkout/[stock-down]` | FailureResilienceUser | Graceful failure when stock is killed |
| `/orders/checkout/[payment-down]` | FailureResilienceUser | Graceful failure when payment is killed |
| `/orders/checkout/[post-stock-recovery]` | FailureResilienceUser | Correct operation after stock restarts |
| `/orders/checkout/[post-payment-recovery]` | FailureResilienceUser | Correct operation after payment restarts |
| `/payment/find_user/[failure-verify]` | FailureResilienceUser | Credit unchanged during stock outage |
| `/stock/find/[failure-verify]` | FailureResilienceUser | Stock rolled back during payment outage |

**Step 5 - Run the locustfile.py:**

```bash
locust -f locustfile.py --host="http://localhost:8000"

or

python -m locust -f locustfile.py --host="http://localhost:8000"
```

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
| Stock Service | Create item, find item, add stock, subtract stock, over-subtract returns 400, stock unchanged after failed subtract |
| Payment Service | Create user, find user, add funds, pay, insufficient credit returns 400, credit unchanged after failed pay |
| Order Service | Create order, find order, add items, checkout failure with stock rollback verification, checkout success with stock and credit verification |
