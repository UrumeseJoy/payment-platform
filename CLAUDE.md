# CLAUDE.md

# Payment Orchestration Platform

## Purpose

This project exists to master production backend engineering concepts and build a flagship resume project for SDE interviews.

The goal is NOT to build another CRUD banking application or a Stripe clone.

The goal is to demonstrate engineering skills such as:

- State machines
- Idempotency
- Event-driven architecture
- Saga pattern
- Double-entry ledger
- Failure handling
- Transaction consistency
- Clean architecture
- Production-quality backend design

Depth is always preferred over breadth.

---

# My Background

I have:

- Backend internship experience at Publicis Sapient on a banking project (Spring Boot, PostgreSQL, Redis, Kafka, Docker)
- Completed a Spring Boot Microservices Banking & Payment Systems course
- Good understanding of Spring Boot, REST APIs, PostgreSQL, Docker, JPA/Hibernate, Redis, Kafka, and backend fundamentals

This project is **NOT** for learning Spring Boot basics.

This project is for understanding how production backend systems are engineered.

---

# Scope Discipline (Read Before Adding Anything)

## Tier 1 (Current Scope)

Only build these:

- Payment State Machine
- Payment REST APIs
- Idempotency
- Double-entry Ledger
- Kafka Event Orchestration
- Retry Handling
- Compensating Transactions (REVERSED)

Nothing else should be added unless explicitly discussed.

---

## Tier 2

Only after Tier 1 is completely implemented and tested.

- Transactional Outbox Pattern
- Dead Letter Queue
- Reconciliation Job

---

## Tier 3

Do NOT build.

Mention only as Future Work.

Examples:

- JWT/Auth
- Real Payment Gateway
- Microservices Split
- Kubernetes
- Prometheus/Grafana
- Circuit Breakers
- OpenTelemetry
- Distributed Tracing

If you think something outside Tier 1 is needed,
STOP and ask before implementing.

---

# Locked Design Decisions

These decisions are final unless I explicitly change them.

## Architecture

- Single Spring Boot application
- Modular Monolith
- NOT Microservices

---

## Package Structure

Package by Feature first.

Example:

payment/
    controller/
    service/
    repository/
    entity/
    dto/

ledger/
    service/
    repository/
    entity/

orchestration/
    service/
    messaging/

gateway/

config/

Cross-cutting concerns belong only inside config/.

---

## Payment State Machine

Legal transitions:

CREATED
    ↓
AUTHORIZED
    ↓
CAPTURED
    ↓
SETTLED

FAILED is reachable from:

- CREATED
- AUTHORIZED

REVERSED is reachable ONLY from:

CAPTURED → REVERSED

There is deliberately NO VOID state in v1.

---

## Ledger

Ledger is:

- Append-only
- Immutable
- Double-entry

Ledger rows are NEVER updated.

Every business event inserts new rows.

Accounts are fixed:

- CUSTOMER_WALLET
- MERCHANT_WALLET
- PLATFORM_SUSPENSE

Every ledger transaction must balance.

Debit total == Credit total

Always.

---

## Idempotency

Two-layer implementation.

Layer 1

Redis

Purpose:

Fast duplicate detection.

Layer 2

Postgres Unique Constraint

Purpose:

Source of truth.

If two identical requests race:

- first succeeds
- second catches unique constraint
- second reloads existing payment
- second returns original response

Never return an error simply because of a race.

---

## Idempotency Reuse

If the same idempotency key is reused with a different request body:

v1 behaviour:

Return the original payment.

Do NOT implement Stripe's 409 conflict behaviour.

---

## Database

Database schema is managed ONLY through Flyway.

Hibernate:

ddl-auto = validate

Hibernate must NEVER generate tables.

---

## Money

Money always uses:

BigDecimal

Database:

NUMERIC(19,4)

Never use:

- float
- double

---

# Coding Philosophy

Prefer:

- Simple
- Clean
- Production-quality
- Readable

Avoid:

- Clever code
- Over-engineering
- Unnecessary abstractions
- Premature optimization

Every class should have one clear responsibility.

Choose the simplest solution that would be acceptable in a real backend team.

---

# Teaching Mode

This project exists so I deeply understand every engineering decision.

Whenever introducing something new:

1. Explain WHY it exists.
2. Explain WHAT problem it solves.
3. Explain WHY we need it in THIS project.
4. Then write the code.

Do not assume I know why a pattern exists.

---

# How We Work Together

Boilerplate is fine to generate completely.

Examples:

- DTOs
- Controllers
- Config classes
- Repository interfaces
- Entity scaffolding
- Exception classes

Core engineering logic should NOT be fully implemented unless I explicitly ask.

Instead:

- create method signatures
- explain the algorithm
- leave TODOs
- let me implement the important logic

Core logic includes:

- State transition validation
- Ledger posting
- Saga compensation
- Retry handling
- Idempotency
- Event orchestration

---

# Engineering Quality

Before writing code:

- Keep methods small.
- Use meaningful names.
- Follow SOLID where appropriate.
- Prefer composition over inheritance.
- Avoid duplication.
- Keep dependencies minimal.

Write code that a backend team would actually merge.

---

# Interview First

Every design decision should optimize for interview quality rather than feature count.

When multiple solutions exist:

Choose the one that creates the strongest engineering discussion.

The project exists to demonstrate backend engineering ability.

---

# Don't Surprise Me

Do NOT without asking:

- Rename packages
- Move files
- Introduce libraries
- Change architecture
- Add design patterns
- Change package structure
- Modify locked design decisions
- Expand project scope

If unsure,

ASK FIRST.

---

# Communication Style

Be direct.

Challenge bad ideas.

Explain trade-offs.

Do not blindly agree with every suggestion.

If something is over-engineered, say so.

If something is not production-worthy, explain why.

Prefer honest engineering feedback over being agreeable.

## Testing
Prefer Testcontainers (real Postgres/Kafka) over mocking for anything that
touches the database or message broker. Mockito is fine for pure logic
(e.g. state transition validation) with no I/O.

## Idempotency cache TTL
Redis idempotency keys expire after 24 hours.