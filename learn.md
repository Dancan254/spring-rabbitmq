# How to Master a New Technology — Applied to RabbitMQ

> **Theory → See it → Build it small → Break it → Build it right → Read a war story**
>
> The biggest trap is spending too long in Phase 1 (reading, watching videos) and never getting to Phase 4. Phase 4 is where understanding becomes instinct.

---

## Phase 1 — Understand the "Why" before the "How"

Read the docs, take notes, draw diagrams. Be able to explain the technology to someone else before writing a line of code. The `README.MD` in this project is exactly this step.

---

## Phase 2 — Run it locally and explore it visually

Before writing any Spring code, spin up RabbitMQ with Docker and open the management UI at `localhost:15672`.

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3-management
```

Manually publish and consume messages through the UI. Seeing messages flow, queue up, and get acked in real time cements what you read far faster than code does.

---

## Phase 3 — Build the official tutorial examples one by one, in order

RabbitMQ has 7 official tutorials. Do each one as a separate, small Spring Boot app:

| # | Tutorial | What you learn |
|---|----------|----------------|
| 1 | Hello World | Basic producer/consumer |
| 2 | Work Queues | Competing consumers, prefetch, ack |
| 3 | Pub/Sub | Fanout exchange |
| 4 | Routing | Direct exchange |
| 5 | Topics | Topic exchange, wildcards |
| 6 | RPC | Request/reply pattern |
| 7 | Publisher Confirms | Reliability on the producer side |

Do them **in order** — each one builds on the last. Don't skip to the interesting one.

---

## Phase 4 — Break things deliberately

This is the step most people skip and it's the most valuable. After each tutorial, intentionally cause failures:

- **Kill the consumer mid-message** — does it requeue?
- **Restart RabbitMQ** — do your queues and messages survive? Test with and without durability.
- **Flood the queue** — publish 10,000 messages, watch memory climb in the management UI.
- **Send a poison message** (one that always throws) — does it loop forever? Now add a DLX and fix it.
- **Run 3 consumers with no prefetch** vs `prefetch=1` — watch the difference in the UI.

Breaking things on purpose is how you build the intuition that saves you at 2am in production.

---

## Phase 5 — Build one reusable reference implementation

Consolidate everything into a single well-structured Spring Boot project (this repo). This is your **personal template** — the project you clone into every future job.

It should include:

- `RabbitMQConfig` — all exchange, queue, and binding declarations in one place
- A producer with publisher confirms and error handling
- A consumer with proper ack/nack and DLX setup
- A retry pattern (DLX → wait queue → re-publish)
- `docker-compose.yml` so you can spin it up anywhere
- Clear comments on the non-obvious config (why `prefetch=1`, why `durable=true`, etc.)

When you revisit this project 6 months later and understand every line, you've mastered it.

---

## Phase 6 — Read one production post-mortem

Search for "RabbitMQ production incident" or "RabbitMQ lessons learned". Reading one real story of what went wrong — memory alarms, unacked message buildup, connection storms — teaches you more about production concerns than any tutorial. It also tells you exactly what to configure defensively in your reference implementation.
