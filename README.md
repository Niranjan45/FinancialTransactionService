# Financial Transaction Processing Service

A production-grade Spring Boot backend simulating a payment gateway / banking transaction processor with high reliability, transactional integrity, idempotency, and fraud validation.

---

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Database Design](#database-design)
- [API Reference](#api-reference)
- [Business Rules & Fraud Detection](#business-rules--fraud-detection)
- [Advanced Features](#advanced-features)
- [Running the Project](#running-the-project)
- [Running Tests](#running-tests)
- [Swagger UI](#swagger-ui)
- [Postman Collection](#postman-collection)
- [Environment Variables](#environment-variables)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        HTTP Client / Postman                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                    Controller Layer (REST)                           │
│         AccountController  │  TransactionController                 │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │                                      │
┌──────────▼──────────────────────────────────────▼───────────────────┐
│                      Service Layer                                   │
│  AccountService │ TransactionService │ FraudDetectionService        │
│  AuditService   │ IdempotencyService │ TransactionEventPublisher    │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │                                      │
┌──────────▼──────────┐               ┌───────────▼───────────────────┐
│  Repository Layer   │               │    External Systems           │
│  (Spring Data JPA)  │               │  ┌──────────┐ ┌────────────┐  │
│                     │               │  │  Redis   │ │   Kafka    │  │
│  AccountRepository  │               │  │  Cache   │ │  Events    │  │
│  TransactionRepo    │               │  └──────────┘ └────────────┘  │
│  AuditRepository    │               └───────────────────────────────┘
│  FraudRuleRepo      │
│  IdempotencyRepo    │
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│     PostgreSQL DB   │
│  accounts           │
│  transactions       │
│  transaction_audit  │
│  fraud_rules        │
│  idempotency_records│
└─────────────────────┘
```

### Key Design Decisions

| Concern | Solution |
|---|---|
| Transactional consistency | `@Transactional` on all write operations; REQUIRES_NEW for audit |
| Concurrent balance updates | Optimistic locking (`@Version`) on Account entity |
| Duplicate payments | Idempotency keys stored in DB with TTL |
| Fraud prevention | Pluggable fraud rules evaluated before every transaction |
| Audit trail | Separate `transaction_audit` table with REQUIRES_NEW propagation |
| Cache | Redis cache on account lookups (5-min TTL), evicted on balance change |
| Event streaming | Kafka topic `transaction-events` on every successful transaction |
| Global error handling | `@RestControllerAdvice` with structured `ApiResponse<T>` |

---

## Technology Stack

| Layer | Technology |
|---|---|
| Framework | Spring Boot 3.2 |
| Language | Java 17 |
| ORM | Spring Data JPA + Hibernate |
| Database | PostgreSQL 15 (MySQL also supported) |
| Migrations | Flyway |
| Cache | Redis |
| Messaging | Apache Kafka |
| Validation | Hibernate Validator (Jakarta) |
| API Docs | SpringDoc OpenAPI 3 (Swagger UI) |
| Metrics | Micrometer + Prometheus |
| Testing | JUnit 5 + Mockito + MockMvc |
| Build | Maven 3.9 |

---

## Project Structure

```
financial-transaction-service/
├── pom.xml
├── fintech-postman-collection.json
├── README.md
└── src/
    ├── main/
    │   ├── java/com/fintech/transaction/
    │   │   ├── FinancialTransactionServiceApplication.java
    │   │   ├── aspect/
    │   │   │   └── ServiceLoggingAspect.java          # AOP method logging
    │   │   ├── config/
    │   │   │   ├── AsyncConfig.java                   # Thread pool for async audit
    │   │   │   ├── JacksonConfig.java                 # Java 8 date/time support
    │   │   │   ├── KafkaConfig.java                   # Topic definitions
    │   │   │   ├── OpenApiConfig.java                 # Swagger metadata
    │   │   │   └── RedisConfig.java                   # Redis cache manager
    │   │   ├── controller/
    │   │   │   ├── AccountController.java
    │   │   │   └── TransactionController.java
    │   │   ├── dto/
    │   │   │   ├── request/
    │   │   │   │   ├── CreateAccountRequest.java
    │   │   │   │   ├── CreditRequest.java
    │   │   │   │   ├── DebitRequest.java
    │   │   │   │   ├── TransactionFilterRequest.java
    │   │   │   │   └── TransferRequest.java
    │   │   │   └── response/
    │   │   │       ├── AccountResponse.java
    │   │   │       ├── ApiResponse.java               # Generic response wrapper
    │   │   │       ├── MetricsResponse.java
    │   │   │       └── TransactionResponse.java
    │   │   ├── entity/
    │   │   │   ├── Account.java                       # @Version optimistic locking
    │   │   │   ├── FraudRule.java
    │   │   │   ├── IdempotencyRecord.java
    │   │   │   ├── Transaction.java
    │   │   │   └── TransactionAudit.java
    │   │   ├── enums/
    │   │   │   ├── AccountStatus.java
    │   │   │   ├── AuditEventType.java
    │   │   │   ├── TransactionStatus.java
    │   │   │   └── TransactionType.java
    │   │   ├── exception/
    │   │   │   ├── AccountInactiveException.java
    │   │   │   ├── AccountNotFoundException.java
    │   │   │   ├── DuplicateAccountException.java
    │   │   │   ├── FraudDetectedException.java
    │   │   │   ├── GlobalExceptionHandler.java        # @RestControllerAdvice
    │   │   │   ├── InsufficientBalanceException.java
    │   │   │   └── TransactionNotFoundException.java
    │   │   ├── repository/
    │   │   │   ├── AccountRepository.java
    │   │   │   ├── FraudRuleRepository.java
    │   │   │   ├── IdempotencyRecordRepository.java
    │   │   │   ├── TransactionAuditRepository.java
    │   │   │   └── TransactionRepository.java
    │   │   ├── scheduler/
    │   │   │   └── ReconciliationScheduler.java       # Daily reconciliation + retry
    │   │   ├── service/
    │   │   │   ├── AccountService.java
    │   │   │   ├── AuditService.java
    │   │   │   ├── FraudDetectionService.java
    │   │   │   ├── IdempotencyService.java
    │   │   │   ├── TransactionEventPublisher.java
    │   │   │   ├── TransactionService.java
    │   │   │   └── impl/
    │   │   │       ├── AccountServiceImpl.java
    │   │   │       └── TransactionServiceImpl.java
    │   │   └── util/
    │   │       ├── AccountMapper.java
    │   │       └── TransactionMapper.java
    │   └── resources/
    │       ├── application.properties
    │       └── db/migration/
    │           ├── V1__init_schema.sql
    │           └── V2__sample_data.sql
    └── test/
        ├── java/com/fintech/transaction/
        │   ├── controller/
        │   │   └── AccountControllerTest.java
        │   └── service/
        │       ├── AccountServiceTest.java
        │       ├── FraudDetectionServiceTest.java
        │       └── TransactionServiceTest.java
        └── resources/
            └── application-test.properties
```

---

## Database Design

### accounts
| Column | Type | Notes |
|---|---|---|
| account_id | UUID PK | Auto-generated |
| account_holder_name | VARCHAR(100) | |
| account_number | VARCHAR(20) UNIQUE | Format: ACC-XXXXXXX |
| balance | NUMERIC(19,4) | CHECK >= 0 |
| currency | VARCHAR(3) | Default: USD |
| status | VARCHAR(20) | ACTIVE/INACTIVE/SUSPENDED/CLOSED |
| version | BIGINT | Optimistic locking |
| created_at / updated_at | TIMESTAMP | Auto-managed |

### transactions
| Column | Type | Notes |
|---|---|---|
| transaction_id | UUID PK | |
| idempotency_key | VARCHAR(100) UNIQUE | Prevents duplicate processing |
| from_account_id | UUID FK | NULL for CREDIT |
| to_account_id | UUID FK | NULL for DEBIT |
| amount | NUMERIC(19,4) | |
| currency | VARCHAR(3) | |
| type | VARCHAR(20) | DEBIT / CREDIT / TRANSFER |
| status | VARCHAR(20) | INITIATED → PROCESSING → SUCCESS/FAILED/REVERSED |
| failure_reason | VARCHAR(500) | |
| retry_count | INT | |

### transaction_audit
| Column | Type | Notes |
|---|---|---|
| audit_id | UUID PK | |
| transaction_id | UUID FK | |
| event_type | VARCHAR(50) | AuditEventType enum |
| message | TEXT | |
| performed_by | VARCHAR(100) | Default: SYSTEM |
| created_at | TIMESTAMP | |

### fraud_rules
| Column | Type | Notes |
|---|---|---|
| rule_id | UUID PK | |
| rule_name | VARCHAR(100) UNIQUE | |
| max_amount_limit | NUMERIC(19,4) | Per-transaction cap |
| max_daily_txn_count | INT | Daily frequency cap |
| max_daily_txn_volume | NUMERIC(19,4) | Daily volume cap |
| enabled | BOOLEAN | Toggle without deletion |

### idempotency_records
| Column | Type | Notes |
|---|---|---|
| idempotency_key | VARCHAR(100) PK | |
| response_body | TEXT | Cached JSON response |
| status_code | INT | |
| expires_at | TIMESTAMP | TTL-based expiry |

---

## API Reference

### Base URL: `http://localhost:8080/api/v1`

#### Accounts

| Method | Endpoint | Description |
|---|---|---|
| POST | `/accounts` | Create account |
| GET | `/accounts/{id}` | Get account by UUID |

#### Transactions

| Method | Endpoint | Description |
|---|---|---|
| POST | `/transactions/debit` | Debit from account |
| POST | `/transactions/credit` | Credit to account |
| POST | `/transactions/transfer` | Transfer between accounts |
| GET | `/transactions/{id}` | Get transaction status |
| GET | `/transactions/history` | Paginated history with filters |
| GET | `/transactions/metrics` | Dashboard metrics |

#### Query Parameters for `/transactions/history`
| Param | Type | Example |
|---|---|---|
| accountId | UUID | Filter by account |
| status | Enum | SUCCESS, FAILED, PROCESSING |
| type | Enum | DEBIT, CREDIT, TRANSFER |
| from | ISO datetime | 2024-01-01T00:00:00 |
| to | ISO datetime | 2024-12-31T23:59:59 |
| page | int | 0 |
| size | int | 20 |
| sortBy | String | createdAt |
| sortDir | String | desc / asc |

### Request / Response Examples

**POST /transactions/debit**
```json
{
  "accountId": "a1000000-0000-0000-0000-000000000001",
  "amount": 500.00,
  "currency": "USD",
  "description": "ATM withdrawal",
  "idempotencyKey": "debit-unique-key-001"
}
```

**Response 201**
```json
{
  "success": true,
  "message": "Debit transaction processed",
  "data": {
    "transactionId": "550e8400-e29b-41d4-a716-446655440000",
    "status": "SUCCESS",
    "amount": 500.00,
    "currency": "USD",
    "type": "DEBIT",
    "createdAt": "2024-05-27T10:30:00"
  },
  "timestamp": "2024-05-27T10:30:00"
}
```

---

## Business Rules & Fraud Detection

1. **Non-negative balance** — `CHECK (balance >= 0)` enforced at DB and service level.
2. **Active accounts only** — Transactions rejected for INACTIVE / SUSPENDED / CLOSED accounts.
3. **Fraud rules evaluated before processing:**
   - `SINGLE_TXN_LIMIT` — rejects any single transaction exceeding ₹10,000 (configurable)
   - `DAILY_TXN_COUNT_LIMIT` — rejects if account has ≥ 10 successful transactions today
   - `DAILY_VOLUME_LIMIT` — rejects if daily volume would exceed ₹50,000
4. **Idempotency** — Repeated requests with the same `idempotencyKey` return the cached response without re-processing.
5. **Optimistic locking** — Concurrent balance updates are detected via `@Version` and return HTTP 409 to the caller.

---

## Advanced Features

### Idempotency
Every mutation endpoint requires an `idempotencyKey` field. Responses are cached in the `idempotency_records` table for 24 hours. Repeated requests return the original response instantly.

### Scheduled Jobs (`ReconciliationScheduler`)
| Job | Schedule | Purpose |
|---|---|---|
| Daily Reconciliation | 01:00 daily | Logs total balance vs. processed volume |
| Retry Failed Transactions | Every 2 hours | Re-queues FAILED txns with retry_count < 3 |
| Idempotency Cleanup | Every hour | Deletes expired idempotency records |

### Retry Mechanism
Failed transactions with `retryCount < maxRetryAttempts` (default: 3) are automatically re-queued by the scheduler. Each retry increments `retryCount` and logs an audit event.

### Optimistic Locking
`Account.version` is a JPA `@Version` field. Concurrent transactions updating the same account will trigger `OptimisticLockingFailureException`, returned as HTTP 409 with error code `CONCURRENT_UPDATE_CONFLICT`.

### Redis Caching
Account lookups are cached in Redis with a 5-minute TTL. Cache is evicted immediately after any successful debit, credit, or transfer that changes the balance.

### Kafka Event Publishing
Every successful transaction publishes an event to the `transaction-events` topic containing:
- `transactionId`, `eventType`, `status`, `amount`, `currency`, `type`, `fromAccountId`, `toAccountId`, `timestamp`

Kafka failures are non-blocking and logged as warnings — they never fail the core transaction.

### AOP Logging
`ServiceLoggingAspect` wraps all service-layer methods with entry/exit/duration logging at DEBUG level.

### Metrics Endpoint
`GET /api/v1/transactions/metrics` returns:
- Total transaction count
- Transactions by status (map)
- Total successful volume
- Total and active account counts

Prometheus metrics are exposed at `/actuator/prometheus`.

---

## Running the Project

### Prerequisites
- Java 17+
- Maven 3.9+
- PostgreSQL 15 (or MySQL 8)
- Redis 7+
- Apache Kafka 3+ (optional — disable in properties if not needed)

### 1. Start Infrastructure with Docker Compose

Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: fintech_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

```bash
docker-compose up -d
```

### 2. Build and Run

```bash
# Clone / navigate to project root
cd financial-transaction-service

# Build (skip tests for first run)
mvn clean package -DskipTests

# Run
java -jar target/financial-transaction-service-1.0.0.jar
```

Or via Maven:
```bash
mvn spring-boot:run
```

### 3. Verify

- API: `http://localhost:8080/api/v1/accounts`
- Swagger UI: `http://localhost:8080/swagger-ui.html`
- Health: `http://localhost:8080/actuator/health`
- Metrics: `http://localhost:8080/actuator/prometheus`

---

## Running Tests

```bash
# All tests
mvn test

# Specific test class
mvn test -Dtest=TransactionServiceTest

# With coverage report
mvn verify
```

Tests use an H2 in-memory database (no external dependencies needed).

---

## Swagger UI

Once running, visit: **http://localhost:8080/swagger-ui.html**

API docs JSON: **http://localhost:8080/v3/api-docs**

---

## Postman Collection

Import `fintech-postman-collection.json` into Postman.

Set the collection variable `baseUrl` to `http://localhost:8080/api/v1`.

The collection includes:
- Account creation and lookup
- Debit, credit, and transfer transactions
- Transaction history with filters
- Metrics dashboard
- Fraud/edge case scenarios (over-limit, insufficient balance, same-account transfer)

---

## Environment Variables

Override any `application.properties` value via environment variable or `-D` flags:

| Property | Default | Description |
|---|---|---|
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/fintech_db` | DB connection |
| `spring.datasource.username` | `postgres` | DB username |
| `spring.datasource.password` | `postgres` | DB password |
| `spring.data.redis.host` | `localhost` | Redis host |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka brokers |
| `idempotency.key.ttl-minutes` | `1440` | Idempotency record TTL (24h) |
| `transaction.retry.max-attempts` | `3` | Max retry count for failed txns |
| `scheduler.reconciliation.cron` | `0 0 1 * * ?` | Reconciliation schedule |

Example override:
```bash
java -jar app.jar \
  --spring.datasource.password=mysecret \
  --spring.data.redis.host=redis.mycompany.com
```
