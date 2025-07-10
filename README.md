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
- If you want to use a type-safe, declarative client for NodeJS, refer to the [NodeJS client docs](#nodejs-client).

## NodeJS Client

The NodeJS client is a straightforward wrapper around the SQL functions with optional type-safety. It uses the `pg` library to connect to Postgres, and provides a simple API for sending and consuming messages.

Before you go ahead and use the client, please ensure that `pgmb` is installed in your postgres database.
```sh
psql postgres://<user>:<pass>@<host>:<port>/<db> -f sql/pgmb.sql -1
```

Note: `psql` must be installed on the machine executing this command.

### Install

1. Make sure you've authenticated to GitHub packages, if not -- you can do so by running:
```sh
npm login --registry=https://npm.pkg.github.com
```

2. Then run:
```sh
npm i @haathie/pgmb pg
```

Or you can install from the github repo directly:
```sh
npm i git+https://github.com/haathie/pgmb.git#main
```

(PG is a peer dependency, so you need to install it separately)

### Connecting

```ts
import { Pool } from 'pg'
import { PGMBClient } from '@haathie/pgmb'

const pgmb = new PGMBClient({
	pool: {
		create: true,
		connectionString: 'postgres://postgres:@localhost:5432/test'
	},
	// leave blank if you don't want to consume any messages
	consumers: []
})
```

Or you can pass in a `pg` pool object:
```ts
import { Pool } from 'pg'
import { PGMBClient } from '@haathie/pgmb'

const pool = new Pool({
	connectionString: 'postgres://postgres:@localhost:5432/test',
	max: 10
})
const pgmb = new PGMBClient({ pool, consumers: [] })
```

### Creating a Queue & Sending Messages

```ts
// simple queue declare
await pgmb.assertQueue({ name: 'my_queue' })
// with more options
await pgmb.assertQueue({
	name: 'my_queue',
	ackSetting: 'archive',
	bindings: ['my_exchange'],
	type: 'unlogged',
	defaultHeaders: {
		// add default headers to all messages for retries
		retriesLeftS: [10, 30, 60]
	}
})

// send multiple messages to the queue
const publishedMsgs = await pgmb.send(
  'my_queue',
  { message: 'Hello', headers: { foo: 'bar' } },
  { message: 'World', headers: { foo: 'baz' } }
)
console.log(publishedMsgs) // [{ id: 'pm123' }, { id: 'pm234' }]
```

### Creating an Exchange & Publishing Messages:

```ts
await pgmb.assertExchange({ name: 'my_exchange' })
// bind a queue to an exchange
await pgmb.bindQueue('my_queue', 'my_exchange')
// publish a message to the exchange
const publishedMsgs = await pgmb.publish(
  { exchange: 'my_exchange', message: 'Hello', headers: { foo: 'bar' } },
  { exchange: 'my_exchange', message: 'World', headers: { foo: 'baz' } }
)
console.log(publishedMsgs) // [{ id: 'pm123' }, { id: 'pm234' }]
```

### Consuming Messages

```ts
// you can create the client with the consumers set. A client can have one
// or more consumers. Each consumer consumes from exactly one queue.
const pgmb = new PGMBClient({
  pool,
  consumers: [
    {
      name: 'my_queue',
			// more options for the queue can also be provided here
			ackSetting: 'archive', // bindings: ['my_exchange'], type: 'unlogged'

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
      onMessage: async ({ queueName, messages }) => {
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

// start listening for messages. Will continue trying to establish a connection
// until successful.
await pgmb.listen()
// set retriesLeft=3 to only retry 3 times before giving up
await pgmb.listen(3)
```

Upon the successful call to listen, the queues will be asserted with any specified
options. The client will then start listening for notifications on the queues.
The client will automatically consume messages in the background, and will
acknowledge them upon successful processing.

#### Partial Acknowledgements

In some cases, a subset of messages may be successfully processed, while others may fail. In such cases, you can acknowledge only the successfully processed messages by using the `ack` method.

```ts
const pgmb = new PGMBClient({
	pool,
	consumers: [
		{
			name: 'my_queue',
			// Optionally, add replicas to the consumer -- to prevent a single batch
			// blocking consumption of other messages.
			replicas: 2,
			// the onMessage fn will have at most <batchSize> messages.
			// If you set the replicas option, the total number of messages
			// being processed will be <batchSize> * <replicas>
			batchSize: 10,
			onMessage: async ({ queueName, messages, ack }) => {
				for(const msg of messages) {
					try {
						await processMessage(message)
						ack(true, msg.id)
					} catch(err) {
						ack(false, msg.id)
					}
				}
			},
		}
	]
})
```

The actual acks/nacks will be transmitted to the database once the `onMessage` callback finally resolves. Any messages that were not ack-d during the `onMessage` callback will be automatically ack-d after the callback resolves, or nack-d if the callback rejects.

### Terminating the Client/Closing the Connection:

```ts
await pgmb.close()
// if you passed in your own pool, you'll need to close it manually
await pool.end()
```

### Type Safety & Serialisers

Most use cases of a message broker have standard messages across exchanges and queues. For example, a queue for user registration may have a standard message format across all exchanges. To enable type safety, you can set generic types when initialising the `PGMBClient`

Since all messages are stored opaquely as `bytea` in Postgres, you'd need a serialiser to convert the messages to and from a string or binary data. The library implements a simple JSON & V8 serialiser -- but it's very easy to implement your own serialiser. See [here](/src/types.ts#L171) for the interface.

For simple queues:
``` ts
import { PGMBClient, JSONSerialiser } from '@haathie/pgmb'

type QueueMap = {
	'queue_1': {
		a: string
	}
	'queue_2': {
		b: string
	}
}

const pgmb = new PGMBClient<QueueMap>({
	pool,
	serialiser: JSONSerialiser,
	consumers: [
		{
			// the name of the queue will be strongly typed
			name: 'queue_2',
			onMessage: async ({ queueName, messages }) => {
				for(const { message, exchange } of messages) {
					// the message having come from an exchange, could
					// be a different type -- so we ignore that
					if(exchange) {
						continue
					}

					// data will be of type QueueMap['queue_2'],
					// will be nicely typed & available for you
					console.log(data.b)
				}
			}
		}
	]
})

// when sending messages, the name of the queue will also be strongly typed
// alongside the message type
await pgmb.send('queue_1', {
	message: { a: 'hello' },
	headers: { foo: 'bar' }
})
```

If you're relying on exchanges to fanout messages, those can also be strongly typed:

``` ts
// below is an example of how types may be defined in a scenario, where
// one service publishes data about new users. And the "email-service" consumes
// the data to send emails to the users.
type QueueMap = {
	// let's say we never want to directly send to a queue
	// but only via an exchange
	'email-service': never
}

type ExchangeMap = {
	'user-registered': {
		id: string
		email: string
	}
}

const pgmb = new PGMBClient<QueueMap, ExchangeMap>({
	pool,
	serialiser: V8Serialiser,
	consumers: [
		{
			name: 'email-service',
			bindings: ['user-registered'],
			onMessage: async ({ queueName, messages }) => {
				for(const { message, exchange } of messages) {
					if(!exchange) {
						continue
					}

					if(exchange === 'user-registered') {
						// data will be of type ExchangeMap['user-registered'],
						// will be nicely typed & available for you
						await sendEmail(message.id, message.email)
					}
				}
			}
		}
	]
})

// publish type safe messages to the exchange
await pgmb.publish(
	{
		exchange: 'user-registered',
		message: { id: '123', email: 'abcd@abcd.com' }
	}
)
```

### Enqueing Messages For Publishing

In some cases, you may want to enqueue messages for publishing & flush them later. The library provides a simple "batcher" API for this. You can use the `batch` method to create a batch, and then use the `flush` method to publish all messages in the batch.

```ts
const pgmb = new PGMBClient({
	...otherOptions,
	batcher: {
		// the batcher will automatically flush messages every 5 seconds
		// set to 0 or undefined to disable auto-flushing
		flushIntervalMs: 5000,
		// the batcher will automatically flush messages when the batch size
		// reaches 2500 messages
		maxBatchSize: 2500
	}
})

pgmb.defaultBatcher.enqueue({
	exchange: 'user-registered',
	message: { id: '123', email: '' }
})

// flush the batch manually
await pgmb.defaultBatcher.flush()
```

Note: if you've messages pending in the batcher, and you close the client, the batcher will automatically flush all messages before closing.

The batcher also automatically logs all failed flushes, in case of errors -- so that they could be recovered manually later.

### General Notes

- **Does the client automatically reconnect on errors & temporary network issues?**
	- Yes, the client will automatically reconnect on errors and temporary network issues. It uses the `pg` library's built-in connection pooling and error handling to manage connections.
- **What happens if the client is terminated while consuming messages?**
	- If the listener was disconnected, the client will automatically reconnect and resume consuming messages from the last acknowledged message.