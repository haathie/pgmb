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
| Archive Table | Messages can be archived to a separate table for later retrieval/audit | ✅ |
| Archive Cleanup/Rollover | The archive table is periodically cleaned up/rolled over to a new table | ❌ |
| Metrics | Basic metrics for monitoring the state of the queues and exchanges | ❌ |

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

TODO: run on semi-large machine

## Installation

The primary code is just SQL functions, that can be utilised in any Postgres database. We've added a client for `NodeJS` for ease of use & some typesafety, but of course, you can use the SQL functions directly in any language that can connect to Postgres.

In Postgres:
- Clone the repository: `git clone https://github.com/haathie/pgmb.git`
- The library must be installed in the database you want to use it in. This can be done by running the following command:
```sh
psql postgres://<user>:<pass>@<host>:<port>/<db> -f sql/pgmb.sql -1
```

## Usage

### Creating a Queue
The most basic usage is to create a queue and publish messages to it. The following example shows how to do this in SQL:
```sql
-- Create a queue. By default the queue will delete messages on acknowledgement.
SELECT pgmb.assert_queue('my_queue');
-- Create a queue that'll archive messages on ack
SELECT pgmb.assert_queue('my_queue', 'archive');
-- Create a queue with some default headers
SELECT pgmb.assert_queue('my_queue', 'archive', '{"foo": "bar"}');
```

### Sending a Message to a Queue

The following example shows how to send a message to a queue:
```sql
-- Send a message to the queue, with no headers and send it now
SELECT pgmb.send('my_queue', ARRAY[('Hello', NULL, NULL)]::pgmb.enqueue_msg[]);
-- Send a message to the queue, with some headers and send it 5 minutes from now
SELECT pgmb.send('my_queue', ARRAY[('Hello', '{"foo": "bar"}', NOW() + interval '5 minutes')]::pgmb.enqueue_msg[]);
```