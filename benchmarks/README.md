# Benchmarks

This folder contains all test suites for the microservices system. There are three categories of tests, each in its own subfolder.

---

## Folder Structure

```
benchmarks/
├── BCT/                        # Basic Correctness Tests
│   ├── test_microservices.py
│   └── utils.py
├── BSCT/                       # Basic Stress and Consistency Tests
│   └── wdm-project-benchmark/
│       ├── consistency-test/
│       │   ├── populate.py
│       │   ├── run_consistency_test.py
│       │   ├── stress.py
│       │   └── verify.py
│       ├── stress-test/
│       │   ├── init_orders.py
│       │   └── locustfile.py
│       ├── requirements.txt
│       └── urls.json
├── PT/                         # Postman Tests
│   └── postman_collection.json
└── README.md
```

---

## Prerequisites

Make sure the full system is running before executing any tests:

```bash
docker compose up --build
```

All tests assume the gateway is reachable at `http://127.0.0.1:8000`. If your setup uses a different address, update `urls.json` (for BSCT) and `BASE_URL` in the Postman collection (for PT).

---

## BCT — Basic Correctness Tests

**Purpose:** Verifies that each microservice behaves correctly in isolation and as a system. Tests cover creating items/users/orders, adding stock, making payments, and a full checkout flow including rollback on failure.

**Location:** `BCT/`

### Setup

```bash
cd BCT
pip install requests
```

### Run

```bash
python test_microservices.py
```

### Interpreting Results

```
...
----------------------------------------------------------------------
Ran 3 tests in 0.121s

OK
```

- `OK` — all 3 tests passed (stock, payment, order)
- `FAIL` — one or more assertions failed, the traceback shows which assertion and what the actual vs expected values were

The three tests are:

| Test | What it checks |
|---|---|
| `test_stock` | Create item, add stock, subtract stock, over-subtract returns 400 |
| `test_payment` | Create user, add credit, pay, checkout flow |
| `test_order` | Full order lifecycle including rollback on out-of-stock and insufficient credit |

---

## BSCT — Basic Stress and Consistency Tests

**Purpose:** Tests the system under concurrent load and verifies that the database remains consistent after many simultaneous requests. This is the most important test suite for validating correctness under stress.

**Location:** `BSCT/wdm-project-benchmark/`

### Setup

```bash
cd BSCT/wdm-project-benchmark
pip install -r requirements.txt
```

Update `urls.json` with your gateway address if needed:

```json
{
  "order_url": "http://127.0.0.1:8000",
  "stock_url": "http://127.0.0.1:8000",
  "payment_url": "http://127.0.0.1:8000"
}
```

---

### Consistency Test

Populates the database with 1 item (100 stock, price 1) and 1000 users (1 credit each), then fires 1000 concurrent checkouts. Only ~10% should succeed. After the run it checks both the service logs and the actual database state for inconsistencies.

```bash
cd consistency-test
python run_consistency_test.py
```

**Interpreting results:**

```
INFO - Consistency test - Starting the consistency evaluation...
INFO - verify - Stock service inconsistencies in the logs: 0
INFO - verify - Stock service inconsistencies in the database: 0
INFO - verify - Payment service inconsistencies in the logs: 0
INFO - verify - Payment service inconsistencies in the database: 0
INFO - Consistency test - Consistency evaluation completed
```

- **Log inconsistencies** — cases where the service returned a success/failure response that didn't match what actually happened
- **Database inconsistencies** — cases where the actual stored state is wrong (e.g. credit deducted but stock not subtracted)
- **Target: 0 database inconsistencies** in both stock and payment

---

### Stress Test

Initializes the database with large-scale data and runs a Locust load test to measure throughput and response times.

**Step 1 — Initialize the database:**

```bash
cd stress-test
python init_orders.py
```

This seeds the database with:

| Setting | Value |
|---|---|
| Items | 100,000 |
| Item stock | 1,000,000 each |
| Item price | 1 |
| Users | 100,000 |
| User credit | 1,000,000 each |
| Orders | 100,000 |

**Step 2 — Run Locust:**

```bash
locust -f locustfile.py --host="http://localhost:8000"
```

For more load, use multiple worker processes:

```bash
locust -f locustfile.py --host="http://localhost:8000" --processes 4
```

**Step 3 — Open the Locust UI:**

Go to `http://localhost:8089` and configure:
- **Number of users** — total concurrent users (start with 50, scale up gradually)
- **Spawn rate** — users added per second (keep under 100 in local mode)
- Click **Start swarming**

**What to monitor in the UI:**

| Tab | What to look for |
|---|---|
| Statistics | Requests/sec, median and 95th percentile response times, failure % |
| Charts | Response time stability as load increases |
| Failures | Any unexpected 4xx or 5xx errors |

A healthy system shows a stable failure rate near 0% and response times that don't grow unboundedly as load increases.

---

## PT — Postman Tests

**Purpose:** Manual and automated API testing for each microservice endpoint. Tests cover happy paths, error cases, and consistency checks (e.g. verifying stock is rolled back after a failed checkout).

**Location:** `PT/postman_collection.json`

### Setup

1. Install [Postman](https://www.postman.com/downloads/)
2. Open Postman → click **Import**
3. Select `PT/postman_collection.json`
4. Click **Import**

Verify the `BASE_URL` variable is set correctly:

1. Click the collection **"Microservices Test Suite"** in the sidebar
2. Go to the **Variables** tab
3. Confirm `BASE_URL` is `http://127.0.0.1:8000`

### Run All Tests

1. Right-click the collection → **Run collection**
2. Ensure **"Keep variable values"** is enabled — requests pass item/user/order IDs to each other
3. Click **Run Microservices Test Suite**

### Interpreting Results

Each request shows green (pass) or red (fail) for every assertion:

```
✅ Create Item                              — Status 200 / Has item_id
✅ Add Stock                                — Status 200
✅ Verify Stock After Add                   — Stock is 50
❌ Checkout - Success                       — Expected status 200, got 400
```

Click any failed request to see the full response body and which assertion failed.

### Test Coverage

| Group | Tests |
|---|---|
| Stock Service | Create, find, add stock, subtract stock, over-subtract returns 400, stock unchanged after failed subtract |
| Payment Service | Create user, find user, add funds, pay, insufficient credit returns 400, credit unchanged after failed pay |
| Order Service | Create order, add items, checkout failure with rollback verification, checkout success, verify stock and credit deducted |
