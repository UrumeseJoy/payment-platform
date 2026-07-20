# Payment orchestration platform

A production-inspired payment orchestration engine, built to learn — and be
able to explain in depth — the backend engineering problems behind real
payment systems: idempotency, event-driven orchestration, saga-style
compensation, and an append-only double-entry ledger.

This is deliberately **not** a Stripe/Razorpay clone and **not** a CRUD app.
The focus is the engineering, not the feature list. Scope was chosen
tier-by-tier — see "Scope and design decisions" below.

## Stack

Java 21, Spring Boot 3, PostgreSQL, Kafka, Redis, Flyway, Testcontainers.

## Running locally

```bash
docker-compose up -d          # Postgres, Redis, Kafka
mvn spring-boot:run           # app runs on localhost:8080 (requires Maven installed)
```

Tip: if you'd rather not install Maven, run `mvn -N wrapper:wrapper` once to
generate `./mvnw`, then use that instead.

Swagger UI: `http://localhost:8080/swagger-ui.html`

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
- `orchestration/` — service (saga/compensation, state transition rules),
  messaging (Kafka producers/consumers)
- `gateway/` — mock payment gateway (injectable success/failure/latency)
- `config/` — cross-cutting Redis/Kafka configuration, shared by every
  feature package rather than owned by one

## Scope and design decisions

_(filled in as we build — this section is what an interviewer will skim
before a call, so keep it honest and specific)_

- **Modular monolith, not microservices**: TODO
- **Single REVERSED state instead of separate VOID/REFUND**: v1 models all
  post-capture reversal as one state. A production system would distinguish
  void (pre-settlement, no funds moved) from refund (post-settlement, funds
  returned) as separate operations with different gateway calls and
  settlement timelines. Deferred deliberately — see conversation history —
  but the ledger schema doesn't foreclose the upgrade.
- **No transactional outbox in v1**: events publish to Kafka post-commit in
  application code. Known gap: a crash between commit and publish loses an
  event. Standard fix is the outbox pattern; scoped out of v1 for time.
- **Flyway over Hibernate auto-ddl**: schema is explicit and versioned
  (`V1__init_schema.sql`) rather than inferred from entities at runtime.
- **Optimistic locking (`@Version`) over pessimistic locking**: TODO — fill
  in once you've tested the concurrent-update scenario.

## What's out of scope (v1)

Auth/JWT, real gateway integration, microservices split, observability
stack (Prometheus/Grafana), circuit breakers. Not gaps — deliberate cuts to
keep the project finishable and defensible in a week.
