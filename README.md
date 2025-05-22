# PGMB - Postgres Message Broker

A lightweight message broker built on top of PostgreSQL. It allows for queues and exchanges, like AMQP systems (RabbitMQ, ActiveMQ, etc.), but uses PostgreSQL as the backend. It is designed to be simple and easy to use, with a focus on performance and reliability.

Presently, it supports the following features:

| Feature | Description | Implemented |
| :------ | :---------- | ----------: |
| Queues | FIFO queues with exactly-once delivery, and multiple consumer support | ✅ |
| Exchanges | A way to route messages to 1 or more queues | ✅ |
| Fanout Bindings | All queues bound to an exchange receive all messages sent to that exchange | ✅ |
| Direct/Topic Bindings | Messages are routed to queues based on a routing key | ❌ |
| Retries | Messages can be retries a configurable number of times before being discarded | ✅ |
| Realtime Consumption | Messages are consumed in real-time using LISTEN/NOTIFY | ✅ |
| Multiple Consumers | Multiple consumers can consume from the same queue | ✅ |
| Scheduled Messages | Messages can be scheduled to be consumed at a later time | ✅ |
| Bulk Publish/Consume | Messages can be published and consumed in bulk | ✅ |
| Unlogged Queues | Use unlogged tables for better performance, in exchange of durability | ✅ |
| Archive Table | Messages can be archived to a separate table for later retrieval/audit | ✅ |
| Archive Cleanup/Rollover | The archive table is periodically cleaned up/rolled over to a new table | ❌ |
| Queue Metrics | Metrics for monitoring the state of queues | ✅ |
| Exchange Metrics | Metrics for monitoring the state of exchanges | ❌ |

## Another Postgres Message Queue Solution?

There are definitely other solutions that use Postgres as a message queue, but they didn't quite fit the bill for our requirements. Here are a few of them:
- [pgmq](https://github.com/pgmq/pgmq): A simple message queue that uses Postgres as the backend. Really loved the feature set & documentation, but
  - lacked an inbuilt fanout
  - lacked the ability to store anything other than a JSON message
  - wasn't sure if this would scale to multiple nodes via something like `citus`. Of course, that'll only be required at a ridiculously large scale, but wanted to just keep that in mind.
- [pgq](https://github.com/pgq/pgq): Probably the best for performance, but didn't seem to be maintained very much + seemed a bit complex to use.
- [pg_mq](https://github.com/perfectsquircle/pg_mq): Almost exactly the feature set required, more powerful than `pgmq` but no longer maintained. Also lacking benchmarks or tests.


Given this, and the simplicity of implementing a message queue on top of Postgres, we decided to build our own. The goal is to keep it simple and easy to use, while also being performant and reliable.

## Benchmarks

Here are benchmarks of PGMB, PGMQ and AMQP. The benchmarks were run on an EC2 server managed by AWS EKS. Each database being allocated `2 cores` and `4GB` of RAM with network mounted EBS volumes. The full details of the benchmarks can be found [here](/docs/k8s-benchmark.md).

| Test | PGMB (delete on ack) | PGMQ (delete on ack) | AMQP (RabbitMQ) |
| :--- | ---: | ---: | ---: |
| msgs published/s | 27321 ± 6493 | 21286 ± 6129 | 27,646 ± 356 |
| msgs consumed/s | 16224 ± 10860 | 3201 ± 5061 | 27,463 ± 392 |

Of course, these benchmarks are for fairly low powered machines, but these give out enough confidence that a Postgres queue can be used as a message broker for reasonably sized workloads. The folks at Tembo managed to squeeze out 30k messages per second, with a more powerful setup. You can find their benchmarks [here](https://hemming-in.rssing.com/chan-2212310/article8486.html?nocache=0)

Note: I'm not super sure why the PGMQ benchmarks are much lower, but I suspect it's due to the fact that it uses a serial ID for messages, has an additional index + I may have not configured it for max performance.

## Getting Started

- If you want to utilise this purely via SQL, refer to the [SQL docs](/docs/sql.md).
- If you want to use a type-safe, declarative client for NodeJS, refer to the [NodeJS client docs](/docs/nodejs.md).