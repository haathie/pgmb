# PGMB - SQL API

## Installation

The primary code is just SQL functions, that can be utilised in any Postgres database. We've added a typescript client for `NodeJS` for ease of use & typesafety, but of course, you can use the SQL functions directly in any language that can connect to Postgres.

In Postgres:
- Clone the repository: `git clone https://github.com/haathie/pgmb.git`
- The library must be installed in the database you want to use it in. This can be done by running the following command:
  ```sh
  psql postgres://<user>:<pass>@<host>:<port>/<db> -f sql/pgmb.sql -1
  ```
  Note: `psql` must be installed on the machine executing this command.

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
-- Create a queue & bind to the specified exchanges. Any exchanges
-- that don't exist will be created. All other exchanges the queue was
-- previously bound to will be unbound.
SELECT pgmb.assert_queue(
  'my_queue',
  bindings => '{my_exchange, my_exchange2}'
);
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
SELECT * FROM pgmb.publish(
  ARRAY[(
    'my_exchange',
    'Hello',
    '{"foo": "bar"}',
    NOW() + interval '5 minutes'
  )]::pgmb.publish_msg[],
);
-- outputs the ID of the published messages (pm123)

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

Notes:
1. The ID of the message when published is the same across all queues that the exchange is bound to. This means if an exchange is bound to queue A, and queue B -- any message M published to the exchange will have the same ID in both queues. This is useful for tracking messages across queues.
2. Because the ID is the same across queues, if you publish 10 messages to an exchange, and 2 queues are bound to that exchange, the `publish` fn will return the unique IDs of the messages in the order they were input to the `publish` fn, i.e. 10 IDs, not 20. However, if the exchange is bound to no queues, the `publish` fn will return no rows.
2. when published from an exchange, the exchange name is added to the message's headers. For eg.
  ```json
  {
    "id": "pm123",
    "headers": {
      "retriesLeftS": [5, 10, 15],
      "exchange": "my_exchange"
    }
  }
  ```
3. Of course, each queue's default headers/archive/type settings are individually respected. So if you have a queue that archives messages, and another that deletes them, the messages will be archived in the first queue and deleted in the second.

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

## ID Generation

All message IDs are generated using the `pgmb.create_message_id` function. Each ID is a sequentially generated ID that is built from the consumption time and some randomness, prefixed by `pm`. I.e. `pm<13 hex digits of consumption ts><7 hex digits of random>`.
When sending/publishing messages in bulk, the `send` and `publish` functions will internally create a "starting" random number, and then increment it for each message sent/published in the batch -- this ensures that the message IDs are all unique, and we retain the order of messages in the batch.