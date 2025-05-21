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

Here are benchmarks of PGMB, PGMQ and AMQP. The benchmarks were run on an EC2 server managed by AWS EKS. Each database being allocated `2 cores` and `4GB` of RAM with network mounted EBS volumes. The full details of the benchmarks can be found [here](/k8s-benchmark/readme.md).

| Test | PGMB (delete on ack) | PGMQ (delete on ack) | AMQP (RabbitMQ) |
| :--- | ---: | ---: | ---: |
| msgs published/s | 27321 ± 6493 | 21286 ± 6129 | 27,646 ± 356 |
| msgs consumed/s | 16224 ± 10860 | 3201 ± 5061 | 27,463 ± 392 |

Of course, these benchmarks are for fairly low powered machines, but these give out enough confidence that a Postgres queue can be used as a message broker for reasonably sized workloads. The folks at Tembo managed to squeeze out 30k messages per second, with a more powerful setup. You can find their benchmarks [here](https://hemming-in.rssing.com/chan-2212310/article8486.html?nocache=0)

Note: I'm not super sure why the PGMQ benchmarks are much lower, but I suspect it's due to the fact that it uses a serial ID for messages, has an additional index + I may have not configured it for max performance.

## Installation

The primary code is just SQL functions, that can be utilised in any Postgres database. We've added a typescript client for `NodeJS` for ease of use & typesafety, but of course, you can use the SQL functions directly in any language that can connect to Postgres.

In Postgres:
- Clone the repository: `git clone https://github.com/haathie/pgmb.git`
- The library must be installed in the database you want to use it in. This can be done by running the following command:
  ```sh
  psql postgres://<user>:<pass>@<host>:<port>/<db> -f sql/pgmb.sql -1
  ```
  note: `psql` must be installed on your machine.

## Testing

1. Clone repository
2. Install dependencies: `npm install`
3. Run test container: `docker compose -f docker/test.docker-compose.yaml up -d`
4. Run tests: `npm test`

## Queues

A queue is a FIFO store of messages. A queue can have multiple simultaneous consumers & publishers. The queue's tables also reside in its own schema, specfied by `pgmb_q_<queue-name>`. The following tables are created:
- `live_messages` => messages that are currently being processed,
- `consumed_messages` => messages that have been consumed & consequently archived -- only if queue's configured to archive.

A queue has the following options:
- **name**: globally unique identifier for the queue
- **ack_setting**: how to handle messages on acknowledgement. The options are:
  - `delete`: delete the message from the queue on acknowledgement
  - `archive`: move the message into the `consumed_messages` table on acknowledgement.
- **default_headers**: default headers to be added to each message put into the queue. This is a JSON object.
- **queue_type**: the type of queue. The options are:
  - `logged`: A normal, WAL logged queue that is durable and will survive a crash
  - `unlogged`: an unlogged queue that is potentially much faster to publish to, but messages will be lost on crash. If `ack_setting` is set to `archive`, the archive table will be unlogged as well. Good read on unlogged tables [here](https://www.crunchydata.com/blog/postgresl-unlogged-tables).

### Live Messages Table

This table contains the messages that are currently being processed. It has the following columns:
- **id**: the ID of the message. This is a sequentially generated ID that is built from the consumption time and some randomness. This single ID lets us have a single field that can be used to identify the message in the queue, and tell us when to consume it. Eg. when we want to consume messages, we simply:
```sql
-- find all messages that are consumable right now, as the message ID
-- generated now is based on the current time.
SELECT * FROM live_messages WHERE id <= pgmb.create_message_id(rand=>9999999);
```
- **headers**: JSONB object containing metadata about the message.
- **message**: the message itself. This is any opaque blob of data.

### Creating a Queue

```sql
-- Create a queue. By default the queue will delete messages on acknowledgement.
SELECT pgmb.assert_queue('my_queue');
-- Create a queue that'll archive messages on ack
SELECT pgmb.assert_queue('my_queue', ack_setting => 'archive');
-- Create a queue with some default headers
SELECT pgmb.assert_queue('my_queue', default_headers => '{"foo": "bar"}');
-- Create an unlogged queue
-- Note: this will not be durable, and messages will be lost on crash,
-- if ack_setting is set to 'archive', the archive table will be unlogged too
SELECT pgmb.assert_queue('my_queue', queue_type => 'unlogged');
```

### Sending Message to a Queue

The following example shows how to send messages to a queue. The API is built for bulk publishing, so you can send multiple messages at once:
```sql
-- Send a message to the queue, with no headers and send it now
SELECT pgmb.send('my_queue', ARRAY[('Hello', NULL, NULL)]::pgmb.enqueue_msg[]);
-- Send a message to the queue, with some headers and consume it 5 minutes
-- from now
SELECT pgmb.send(
  'my_queue',
  ARRAY[(
    'Hello',
    '{"foo": "bar"}',
    NOW() + interval '5 minutes'
  )]::pgmb.enqueue_msg[]
);
-- Send multiple messages to the queue, with some headers:
SELECT pgmb.send('my_queue', ARRAY[
  ('Hello', '{"foo": "bar"}', NULL),
  ('World', '{"tenantId": "1234"}', NOW() + interval '5 minutes'),
  ('!', '{"a": "b"}', NULL)
]::pgmb.enqueue_msg[]);
```

Just as an example, if the queue has default headers set to `{"foo": "bar"}`, and you send a message with the headers `{"a": "b"}`, like:
``` sql
SELECT pgmb.send('my_queue', ARRAY[
  ('Hello', '{"a": "b"}', NULL)
]::pgmb.enqueue_msg[]);
```

The final message will be:
```json
{
  "id": "pm123",
  "headers": {
    "foo": "bar",
    "a": "b"
  },
  "message": "Hello"
}
```

Each queue also has its own channel, specified by `chn_<queue_name>`. Whenever messages are sent to the queue via the `send` fn -- a notification is sent to the channel. This can be used to notify consumers that there are messages in the queue. The notification payload is a simple JSON `{"count": <number of messages>}`. This is useful for realtime consumption of messages.

### Acknowledging Messages

#### Positive Acknowledgement:

Mark the message as successfully processed -- this will delete the message from the queue. If the queue's ack setting is set to archive, the message will be moved to the archive table with the "success" flag as true.
```sql
-- Positively ack messages with IDs pm123 and pm234
SELECT pgmb.ack_msgs('my_queue', true, ARRAY['pm123', 'pm234']);
```

#### Negative Acknowledgement:

This will delete the message from the queue. If the queue's ack setting is set to archive, the message will be moved to the archive table with the "success" flag as false.

```sql
-- Negatively ack messages with IDs pm123 and pm234
SELECT pgmb.ack_msgs('my_queue', false, ARRAY['pm123', 'pm234']);
```

**Retries:** you can configure a message to be retried a certain number of times before being discarded. This is done by setting the `retriesLeftS` header to a list of integers that specify how much time to wait before retrying the message in seconds. For example, if you set the header to `[5, 10, 15]`, the message will be retried after 5 seconds, then 10 seconds, then 15 seconds. If the message still fails after all retries, it will be discarded. The retries are set in the header of the message, so you can set them when you send the message.

Note: as the message's ID determines when it'll be consumed, this current message is discarded and a new message is created with a new ID. The original ID is available via the `originalMessageId` header. Let's clarify via an example:

```sql
-- send a message with retries
SELECT pgmb.send('my_queue', ARRAY[
  ('Hello', '{"retriesLeftS": [5, 10, 15]}', NULL)
]::pgmb.enqueue_msg[]);

-- let's say it failed
SELECT pgmb.ack_msgs('my_queue', false, ARRAY['pm123']);
-- this will create a new message with the original ID in the header &
-- the first retry popped
-- eg. (
--  id: pm234,
--  headers: {"retriesLeftS": [10, 15], "originalMessageId": "pm123"},
--  message: "Hello"
-- )
```

### Getting Metrics of Queues

```sql
SELECT * FROM get_queue_metrics('my_queue');
```
This will return a table like this:
| queue_name | total_length | consumable_length | newest_msg_age | oldest_msg_age |
| :---------- | ------------: | ----------------: | --------------: | --------------: |
| my_queue    | 3             | 2                 | - 10 seconds    | 300 minutes     |

**total_length**: The total number of messages in the queue, including those that are not consumable (i.e. scheduled for future consumption).
**consumable_length**: The number of messages that are consumable right now.
**newest_msg_age**: How far away is the newest message in the queue from being consumable. This is a negative number if the message is scheduled for future consumption.
**oldest_msg_age**: How far away is the oldest message that is ready to be consumed, this tells you how long the oldest message has been in the queue.

### Other Utility Functions for Queues

```sql
-- Delete a queue. This will delete all messages in the queue.
SELECT pgmb.delete_queue('my_queue');
-- Purge a queue. This will delete all live messages in the queue, but not the queue itself. Does not affect the archive data.
SELECT pgmb.purge_queue('my_queue');
```

## Exchanges

An exchange is a way to route messages to one or more queues -- very similar to the concept of exchanges in AMQP. An exchange can be bound to one or more queues, and messages sent to the exchange will be routed to all bound queues -- i.e. a fanout exchange.

An exchange has the following options:
- **name:** globally unique identifier for the exchange
- **queues:** list of queues that receive messages whenever a message is sent to the exchange.

### Creating and Publishing to an Exchange

```sql
-- Create an exchange.
SELECT pgmb.assert_exchange('my_exchange');

-- Bind a queue to an exchange (of course, the queue must exist).
SELECT pgmb.bind_queue('my_queue', 'my_exchange');

-- Publish a message to the exchange. Again, the API is built for bulk 
-- publishing, so you can send multiple messages at once to multiple
-- exchanges:
SELECT pgmb.publish(
  ARRAY[(
    'my_exchange',
    'Hello',
    '{"foo": "bar"}',
    NOW() + interval '5 minutes'
  )]::pgmb.publish_msg[],
);

SELECT pgmb.publish(
  ARRAY[
    (
      'my_exchange',
      'Hello',
      '{"foo": "bar"}',
      NOW() + interval '5 minutes'
    ),
    (
      'my_exchange2',
      'Hello world',
      NULL,
      NULL
    )
  ]::pgmb.publish_msg[],
);
```

Note: when published from an exchange, the exchange name is added to the message's headers. For eg.
```json
{
  "id": "pm123",
  "headers": {
    "retriesLeftS": [5, 10, 15],
    "exchange": "my_exchange"
  }
}
```

And of course, each queue's default headers/archive/type settings are individually respected. So if you have a queue that archives messages, and another that deletes them, the messages will be archived in the first queue and deleted in the second.

### Unbinding a Queue to an Exchange

```sql
-- Bind a queue to an exchange.
SELECT pgmb.unbind_queue('my_queue', 'my_exchange');
```

### Deleting an Exchange

```sql
SELECT pgmb.delete_exchange('my_exchange');
```

## Uninstalling PGMB

To uninstall PGMB, you can run the following command:
```sql
SELECT pgmb.uninstall();
```

This will remove the `pgmb` schema, and all queue schemas -- thus removing all data too. If you just want to remove the queues, you can just drop the `pgmb` schema:
```sql
DROP SCHEMA pgmb CASCADE;
```

## NodeJS Client

The NodeJS client is a simple wrapper around the SQL functions. It uses the `pg` library to connect to Postgres, and provides a simple API for sending and consuming messages.

You can install the client using npm:
```sh
npm install @haathie/pgmb
```

Connecting:
```ts
import { Pool } from 'pg'
import { PGMBClient } from '@haathie/pgmb'

const pool = new Pool({
  connectionString: 'postgres://postgres:@localhost:5432/test',
  max: 10
})

const pgmb = new PGMBClient({ pool })
```

Creating a queue & sending messages:
```ts
await pgmb.assertQueue({ name: 'my_queue' })
// send multiple messages to the queue
const publishedIds = await pgmb.send(
  'my_queue',
  { message: 'Hello', headers: { foo: 'bar' } },
  { message: 'World', headers: { foo: 'baz' } }
)
console.log(publishedIds) // [{ id: 'pm123' }, { id: 'pm234' }]
```

Creating an exchange & publishing messages:
```ts
await pgmb.assertExchange({ name: 'my_exchange' })
// bind a queue to an exchange
await pgmb.bindQueue('my_queue', 'my_exchange')
// publish a message to the exchange
const publishedIds = await pgmb.publish(
  { exchange: 'my_exchange', message: 'Hello', headers: { foo: 'bar' } },
  { exchange: 'my_exchange', message: 'World', headers: { foo: 'baz' } }
)
console.log(publishedIds) // [{ id: 'pm123' }, { id: 'pm234' }]
```

Consuming messages:
```ts
// you can create the client with the consumers set. A client can have one
// or more consumers. Each consumer consumes from exactly one queue.
const pgmb = new PGMBClient({
  pool,
  consumers: [
    {
      queueName: 'my_queue',
      // the onMessage fn will have at most <batchSize> messages
      // in the messages array. The messages are guaranteed to be
      // consumable at the time of consumption.
      // Internally, the pgmb client fetches these many messages
      // in a single query
      batchSize: 10,
      // optionally, will wait for this long after receiving a notification
      // or if batchSize is reached, before consuming messages
      debounceIntervalMs: 1000,
      // process the messages, upon successful resolution of the
      // promise, the messages will be acknowledged. If the promise
      // is rejected, the messages will be negatively acknowledged.
      onMessage: async (queueName, messages) => {
        /**
         * [{
         *  id: 'pm123',
         *  headers: { foo: 'bar' },
         *  message: Buffer.from('hello')
         * }]
         */
        console.log(messages)
      },
    }
  ]
})

// start listening for messages
await pgmb.listen()
```

Upon the successful call to listen, all consumers will consume any pending messages in the queue. Note, that the queues must exist before calling `listen()`, otherwise you will get an error in the consumer.

If you need to in some way alter which queues are being consumed, you can do so by calling `pgmb.replaceConsumers()`. This will stop the current consumers and start the new ones. The new consumers must be passed in the same format as the original ones.

``` ts
// will also automatically call listen() for you, so no need to call it again
await pgmb.replaceConsumers(
  {
    queueName: 'my_queue',
    batchSize: 10,
    debounceIntervalMs: 1000,
    onMessage: async (queueName, messages) => { /** ... */ },
  },
  {
    queueName: 'my_queue_2',
    batchSize: 1000,
    onMessage: async (queueName, messages) => { /** ... */ },
  }
)
```

Terminating the client:
```ts
await pgmb.close()
await pool.end()
```