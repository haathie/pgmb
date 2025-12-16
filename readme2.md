# PGMB - Postgres Message Broker

A heavyweight message broker & event log built on top of PostgreSQL. PGMB tries to do most of the computational work in Postgres itself, and leaves the IO work to the Javscript runtime. PGMB guarantees **at-least-once** delivery of messages & mostly ordered delivery.

Using this package you can implement:
1. Queue and exchange like behaviour seen in AMQP systems (RabbitMQ, ActiveMQ, etc.) with retries & reliable message delivery.
2. Batch publish messages.
3. Automatic insert, update, delete events for any table.
4. HTTP SSE (Server Sent Events) for an arbitrary Postgres query, with resumption support via the standard `Last-Event-ID` header.
5. Webhooks for events, again based on arbitrary Postgres queries, with retry logic.

## Benchmarks

Here are benchmarks of PGMB, PGMQ and AMQP. The benchmarks were run on an EC2 server managed by AWS EKS. Each database being allocated `2 cores` and `4GB` of RAM with network mounted EBS volumes. The full details of the benchmarks can be found [here](/docs/k8s-benchmark.md).

| Test | PGMB | PGMQ (delete on ack) | AMQP (RabbitMQ) |
| :--- | ---: | ---: | ---: |
| msgs published/s | 27321 ± 6493 | 21286 ± 6129 | 27,646 ± 356 |
| msgs consumed/s | 16224 ± 10860 | 3201 ± 5061 | 27,463 ± 392 |

Of course, these benchmarks are for fairly low powered machines, but these give out enough confidence that a Postgres queue can be used as a message broker for reasonably sized workloads. The folks at Tembo managed to squeeze out 30k messages per second, with a more powerful setup. You can find their benchmarks [here](https://hemming-in.rssing.com/chan-2212310/article8486.html?nocache=0)

Note: I'm not super sure why the PGMQ benchmarks are much lower, but I suspect it's due to the fact that it uses a serial ID for messages, has an additional index + I may have not configured it for max performance.

## Setup

First, let's ensure that PGMB is installed in your Postgres database. PGMB is just SQL code, so can be installed in limited environments where you may not have superuser access. PGMB is also compatible with [PGLite](https://pglite.dev).

Once you've the repository cloned:
```sh
psql postgres://<user>:<pass>@<host>:<port>/<db> -f sql/pgmb.sql -1
```

Initial setup is now complete. You can now use the package in your Node.js project.

```sh
npm install @haathie/pgmb
```

``` ts
import { Pool } from 'pg'
import { PgmbClient } from '@haathie/pgmb'

const pool = new Pool({ connectionString: 'postgres://...' })
const pgmb = new PgmbClient({
	client: pool,
	// Provide a unique identifier for this
	// instance of the PGMB client. This should
	// be globally unique in your system.
	groupId: 'my-node',
})
await pgmb.init()

process.on('SIGINT', async () => {
	// gracefully end the client on process exit
	// ensures any current work is finished, before
	// exiting.
	await pgmb.end()
	await pool.end()
	process.exit(0)
})
```

Typically, we'd like our events to be typed. This can be achieved by providing a type parameter to the `PgmbClient` class. For example, if our events have the following structure:
``` ts
interface MyEventData {
	topic: 'user-created'
	payload: {
		id: number
		name: string
		email: string
	}
} | {
	topic: 'order-placed'
	payload: {
		orderId: number
		userId: number
		amount: number
	}
}

// when publishing or consuming events,
// TypeScript will now ensure type-safety
const pgmb = new PgmbClient<MyEventData>({ ... })
```

## Basic Concepts

Full details about the architecture & design of PGMB can be found in the [docs](/docs/sql.md). At a high level, PGMB revolves around the concept of **events** & **subscriptions**, all features emerge from the interaction between these two concepts.

All events are stored in a table called `pgmb.events`. This table is an insert-only log of all events that have been published. It's automatically partitioned, and requires no autovacuumming -- to maximise throughput & compute utilisation. Each event is written only once to this table, and retreived via a join.

Subscriptions are stored in the `pgmb.subscriptions` table. Each subscription defines a set of conditions that determine which events it is interested in, this is specified by the "conditions_sql" and "params" columns. "conditions_sql" is a SQL expression that is evaluated for each event, and if it returns true, the event is considered to match the subscription. "params" is a JSONB object that can be used to parameterise the "conditions_sql". To make this operation efficient & not bloat to N^2 complexity, a GIN index is created on the "params" column, and the "conditions_sql" should be written in a way that can leverage this index.

Subscriptions are grouped by a "group_id", a group owns a set of subscriptions, and each instance of a PGMB client should have a globally unique & persistent "group_id". This allows 

Too minimise the number of unique SQL queries that have to run to match events to subscriptions, PGMB automatically groups subscriptions that have the same "conditions_sql" together. This means that if you have multiple subscriptions with the same "conditions_sql", they will be evaluated together, reducing the number of matching operations required.
Furthermore, each subscription is uniquely stored on (group, conditions_sql, params). This means that if multiple consumers require listening to the same set of events, they share the same underlying subscription, thereby reducing duplication of work.

Subscriptions can also be automatically expired after a certain period of inactivity (useful for temporary consumers like HTTP SSE connections).

## Building a Queue & Exchange system

``` ts
import { createRetryHandler } from '@haathie/pgmb'

// setup a reliable consumer. Should execute this on each boot of your
// service that wants to consume messages.
const consumer = await pgmb.registerReliableSubscription(
	{
		// we'll listen to "msg-created" and "msg-updated" events.
		// note: this SQL leverages the GIN index on "params" column
		conditionsSql: "s.params @> jsonb_build_object('topics', ARRAY[e.topic])",
		params: { topics: ['msg-created', 'msg-updated'] },
		// if the subscription isn't actively handled for more than 15 minutes,
		// it'll be expired & removed. We'll put this here in case the conditions,
		// or parameters change in the future, the stale subscription will be
		// removed automatically.
		expiryInterval: '15 minutes'
	},
	// ideally add a retry handler, this will ensure that
	// any failures in processing are retried based on your
	// provided configuration.
	// Each retry will include the exact same set of events
	// that were included in the original attempt.
	createRetryHandler(
		{
			// will retry after 1 minute, then after 5 minutes
			retriesS: [60, 5 * 60]
		},
		async({ items }, { logger }) => {
			for(const item of items) {
				logger.info('Received event', item.payload)
			}
		}
	)
)

// let's publish an event
await pgmb.publish([{ topic: 'msg-created', payload: { hello: 'world' } }])
```

As PGMB only supports a single consumer per subscription, if you need to scale out consumption, you can partition the subscription such that an approximately equal number of events are routed to each consumer. For example:
``` ts
// get the worker number from environment variable
const workerNumber = +(process.env.WORKER_NUMBER ?? 0)

const pgmb = new PgmbClient({
	groupId: `my-node-worker-${workerNumber}`,
	...otherOpts
})

await pgmb.registerReliableSubscription(
	{
		// we're partitioning by the event ID here, but it's just as
		// easy to partition by any other attribute of the event.
		// note: again this SQL leverages the GIN index on "params" column
		conditionsSql: `s.params @> jsonb_build_object(
			'topics', ARRAY[e.topic],
			'partition', hashtext(e.id) % 3
		)`,
		params: {
			topics: ['msg-created', 'msg-updated'],
			partition: workerNumber
		},
		expiryInterval: '15 minutes'
	},
	handler
)
```

As registering topical subscriptions is a common use case, PGMB provides a helper function to create such subscriptions easily.

``` ts
import { createTopicalSubscriptionParams } from '@haathie/pgmb'

const sub = await pgmb.registerReliableSubscription(
	createTopicalSubscriptionParams({
		topics: ['msg-created', 'msg-updated'],
		partition: { current: workerNumber, total: 3 },
		expiryInterval: '15 minutes'
	}),
	handler
)
```

## Publishing Messages

As shown above, it's quite easy to publish a message or two:
``` ts
await pgmb.publish(
	[
		{ topic: 'msg-created', payload: { id: 123, hello: 'world' } },
		{ topic: 'msg-updated', payload: { id: 123, status: 'active' } }
	],
	// optionally provide a PG client, if publishing
	// in a transactional context
	txClient
)
```

If the client is typed, then you'll have type-safety while publishing messages too. If you need to publish a large number of messages at different points in your code and do not want to block the main request, PGMB can enqueue messages to be published in the background. 
``` ts 
const pgmb = new PgmbClient({
	...otherOpts,
	// flush every second
	flushIntervalMs: 1000,
	// or if there are 100 messages queued
	maxBatchSize: 100,
})

// enqueue messages to be published in the background
pgmb.enqueue({ topic: 'msg-created', payload: { id: 123, hello: 'world' } })
```

In case of failures while publishing messages in the background, PGMB will log the full failed message so it can be picked up later.

## Automatic Table Events

If you need to automatically produce events whenever a table is mutated (insert, update, delete), PGMB makes it super easy to do so:

``` sql
-- let's say we have a table "users"
CREATE TABLE users (
	id SERIAL PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	name TEXT NOT NULL,
	email TEXT NOT NULL UNIQUE
);

-- to enable automatic events from mutations on this table,
-- we just need to call the following function:
SELECT pgmb.push_table_mutations('public.users'::regclass);

-- to disable certain mutation types, we can parameters for the same.
-- for example, to only enable "insert" events:
SELECT pgmb.push_table_mutations(
	'public.users'::regclass,
	delete := FALSE,
	update := FALSE
);
```

This will automatically create statement level triggers on the `users` table,
and push events whenever a mutation occurs. The following events are produced:
- `<schema>.<table>.insert` (eg. `public.users.insert`)
- `<schema>.<table>.update` (eg. `public.users.update`)
- `<schema>.<table>.delete` (eg. `public.users.delete`)

As events are published via triggers in the same transaction as the mutation,
you can be sure that if the transaction rolls back, no events are published.

For any given table, you can obtain the event's schema using:
``` ts
import { PgmbClient, ITableMutationEventData } from '@haathie/pgmb'

type UserEventData = ITableMutationEventData<
	// ideally the table's row type comes from some
	// type generator like pgtyped, kanel, etc
	{
		id: number
		created_at: string
		name: string
		email: string
	},
	'public.users'
>

// then if you create the client as shown before:
const pgmb = new PgmbClient<UserEventData>({ ... })

// you can now consume events with proper typing:
const sub = await pgmb.registerFireAndForgetSubscription({})
for await(const { items } of sub) {
	for(const item of items) {
		if(item.topic === 'public.users.insert') {
			// TypeScript knows that item.payload is of type
			console.log('New user created w ID:', item.payload.id)
		}
	}
}
```

### Customising Table Events

Sometimes, it's not desirable to publish events for all mutations on a table, but only a subset of them. Or perhaps some transformations need to be applied to the data before publishing. To achieve this, PGMB allows you to create custom functions that determine whether to publish an event or not + have the ability to serialise the data to JSON on your own.

TODO
