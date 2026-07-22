# Payment orchestration platform

A production-inspired payment orchestration engine, built to learn — and be
able to explain in depth — the backend engineering problems behind real
payment systems: idempotency, event-driven orchestration, saga-style
compensation, and an append-only double-entry ledger.

This is deliberately **not** a Stripe/Razorpay clone and **not** a CRUD app.
The focus is the engineering, not the feature list. Scope was chosen
tier-by-tier — see "Scope and design decisions" below.

## Stack

Java 21 (bytecode target), Spring Boot 3, PostgreSQL, Kafka, Redis, Flyway,
Testcontainers. Developed on JDK 25 — see the JDK note below.

## Running locally

```bash
docker-compose up -d          # Postgres, Redis, Kafka
mvn spring-boot:run           # app runs on localhost:8080 (requires Maven installed)
```

Tip: if you'd rather not install Maven, run `mvn -N wrapper:wrapper` once to
generate `./mvnw`, then use that instead.

Swagger UI: `http://localhost:8080/swagger-ui.html`

## API

- `POST /payments` — create a payment. Requires an `Idempotency-Key` header;
  duplicate requests with the same key return the original payment rather
  than creating a new one.
- `POST /payments/{id}/reverse` — reverse a captured payment. Only legal for
  payments currently in `CAPTURED` status; returns `409 Conflict` otherwise,
  `404` if the payment doesn't exist. Returns `202 Accepted`, not `200`,
  because the reversal is applied asynchronously by the Kafka consumer —
  the response confirms the request was valid and queued, not that the
  reversal has completed.

## Architecture

Single modular monolith, not microservices — see design decisions below for
why. Packages are organized **by feature first, then by layer** (rather than
one flat `controller`/`service`/`repository` split), because this service
has several distinct business capabilities and grouping by feature keeps
each one navigable on its own:

- `payment/` — controller, service, repository, entity (`Payment`,
  `PaymentStatus`), dto — the payment lifecycle and its REST API
- `ledger/` — repository, entity (`LedgerEntry`, `LedgerAccount`,
  `EntryType`), service — double-entry posting logic
- `orchestration/` — messaging (Kafka event model, producer, consumer) —
  the saga/compensation flow and the state-transition orchestration
- `gateway/` — deterministic mock payment gateway
- `config/` — cross-cutting Redis/Kafka configuration, shared by every
  feature package rather than owned by one

### Payment lifecycle

```
CREATED --> AUTHORIZED --> CAPTURED --> SETTLED (future work, see below)
   |             |             |
   v             v             v
FAILED        FAILED       REVERSED
```

`REVERSED` is reachable only from `CAPTURED`. Transitions are validated and
enforced by `Payment.transitionTo`, not just implied by an unguarded status
field.

## Scope and design decisions

- **Modular monolith, not microservices**: at this scope, splitting into
  services would trade real engineering problems (idempotency, saga
  compensation, ledger correctness) for premature infrastructure concerns
  (service discovery, network partitioning between services that don't
  need to be separate). A well-modularized monolith demonstrates the same
  domain boundaries without the operational overhead a project this size
  doesn't need.

- **Single REVERSED state instead of separate VOID/REFUND**: v1 models all
  post-capture reversal as one state. A production system would distinguish
  void (pre-settlement, no funds moved) from refund (post-settlement, funds
  returned) as separate operations with different gateway calls and
  settlement timelines. Deferred deliberately, but the ledger schema
  doesn't foreclose the upgrade.

- **Only CAPTURED and REVERSED post ledger entries**: AUTHORIZED represents
  a hold, not a booked money movement, so it doesn't touch the ledger. This
  avoids needing a reversal entry to "release" a hold when a payment fails
  after authorization — CREATED/AUTHORIZED/FAILED never touch the ledger at
  all in v1.

- **SETTLED is no longer auto-triggered after CAPTURED**: v1 initially
  chained CAPTURED -> SETTLED automatically, treating settlement as
  instantaneous. This was inaccurate — real settlement is a separate,
  often batched process (T+1/T+2), and modeling it as instant also meant
  CAPTURED was never a stable, observable state, closing off the reversal
  path in practice. v1 now stops at CAPTURED; a settlement batch job
  (future work) would be responsible for the CAPTURED -> SETTLED
  transition.

- **No transactional outbox in v1**: events publish to Kafka post-commit in
  application code. Known gap: a crash between commit and publish loses an
  event. Standard fix is the outbox pattern; scoped out of v1 for time.

- **No Dead Letter Queue in v1**: when Kafka retries are exhausted (3
  attempts, exponential backoff), Spring Kafka's default behavior is to log
  and move on. A DLQ topic for manual triage of permanently-failed events
  is Tier 2 future work.

- **Flyway over Hibernate auto-ddl**: schema is explicit and versioned
  (`V1__init_schema.sql`) rather than inferred from entities at runtime.

- **Optimistic locking (`@Version`) over pessimistic locking**: chosen
  because payment updates are expected to rarely collide in practice (one
  event pipeline per payment, ordered by Kafka partition key), so paying
  the cost of a DB row lock on every write isn't justified. `LedgerService`
  retries up to 3 times on `OptimisticLockingFailureException`, reloading
  the fresh row each attempt rather than blindly reapplying stale data —
  proven under concurrent load in `PaymentServiceIdempotencyTest`.

- **Deterministic mock gateway over random failure injection**: the mock
  gateway triggers DECLINED/TIMEOUT outcomes based on the payment amount's
  cents value (`.13` / `.99`), modeled after Stripe's test-card convention.
  This makes every failure scenario reproducible on demand rather than
  relying on random chance, which would make tests flaky.

## JDK note

Developed and tested on JDK 25; the build targets Java 21 bytecode
(`java.version` in `pom.xml`) for broader compatibility with typical
production environments. Lombok and Mockito versions were pinned above
their Spring Boot 3.3.4-managed defaults (`lombok.version`,
`mockito.version` in `pom.xml`) because both libraries' bundled bytecode
tooling (Lombok's annotation processor, Mockito's Byte Buddy) didn't yet
support JDK 25's class file format at those default versions — a real,
current compatibility gap as JDK 25 adoption is still ramping up.

## What's out of scope (v1)

Auth/JWT, real gateway integration, microservices split, observability
stack (Prometheus/Grafana), circuit breakers. Not gaps — deliberate cuts to
keep the project finishable and defensible in a week.