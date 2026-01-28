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

Install PGMB by running the following command:

```sh
npm install @haathie/pgmb
```

Before using PGMB, you'll need to run the setup script to create the required tables, functions & triggers in your database. You can do this by running:

```sh
psql postgres://<user>:<pass>@<host>:<port>/<db> -f node_modules/@haathie/pgmb/sql/pgmb.sql -1
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

Full details about the architecture, design & performance tricks of PGMB can be found in the [docs](/docs/arch.md). At a high level, PGMB revolves around the concept of **events** & **subscriptions**, all features emerge from the interaction between these two concepts.

All events are stored in a table called `pgmb.events`. This table is an insert-only log of all events that have been published.

Subscriptions are stored in the `pgmb.subscriptions` table. Each subscription defines a set of conditions that determine which events it is interested in, by the "conditions_sql" and "params" columns. "conditions_sql" is a SQL expression that is evaluated for each event, and if it returns true, the event is considered to match the subscription. "params" is a JSONB object that can be used to parameterise the "conditions_sql".

Subscriptions are grouped by a "group_id", a group owns a set of subscriptions, and each instance of a PGMB client should have a globally unique & persistent "group_id".

To minimise the number of unique SQL queries that have to run to match events to subscriptions, PGMB automatically groups subscriptions that have the same "conditions_sql" together. This means to optimise, try to utilise the same SQL query to match events, and then utilise "params" to differentiate between different subscriptions.
Furthermore, each subscription is uniquely stored on (group, conditions_sql, params). This means that if multiple consumers require listening to the same set of events, they share the same underlying subscription, thereby reducing duplication of work.

Subscriptions can also be automatically expired after a certain period of inactivity (useful for temporary consumers like HTTP SSE connections).

## Building a Queue & Exchange system

``` ts
// setup a reliable consumer. Should execute this on each boot of your
// service that wants to consume messages.
await pgmb.registerReliableHandler(
	{
		// we'll listen to "msg-created" and "msg-updated" events.
		// note: this SQL leverages the GIN index on "params" column
		conditionsSql: "s.params @> jsonb_build_object('topics', ARRAY[e.topic])",
		params: { topics: ['msg-created', 'msg-updated'] },
		// if the subscription isn't actively handled for more than 15 minutes,
		// it'll be expired & removed. We'll put this here in case the conditions,
		// or parameters change in the future, the stale subscription will be
		// removed automatically.
		expiryInterval: '15 minutes',
		// give a unique name for this handler. It need only be unique
		// within the same subscription parameters
		name: 'log-data',
		// Each retry will include the exact same set of events
		// that were included in the original attempt.
		retryOpts: {
			// will retry after 1 minute, then after 5 minutes
			retriesS: [60, 5 * 60]
		},
		// optionally provide a splitBy function to split
		// event to be processed differently based on some attribute.
		splitBy(ev) {
			return Object.values(
				// group events by their topic
				ev.items.reduce((acc, item) => {
					const key = item.topic
					acc[key] ||= { items: [] }
					acc[key].items.push(item)
					return acc
				}, {})
			)
		}
	},
	async({ items }, { logger }) => {
		for(const item of items) {
			logger.info('Received event', item.payload)
		}
	}
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

await pgmb.registerReliableHandler(
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
		expiryInterval: '15 minutes',
		...otherParams
	},
	handler
)
```

As registering topical subscriptions is a common use case, PGMB provides a helper function to create such subscriptions easily.

``` ts
import { createTopicalSubscriptionParams } from '@haathie/pgmb'

const sub = await pgmb.registerReliableHandler(
	createTopicalSubscriptionParams({
		topics: ['msg-created', 'msg-updated'],
		partition: { current: workerNumber, total: 3 },
		expiryInterval: '15 minutes'
	}),
	handler
)
```

If your use case doesn't need reliable processing of messages, and you just want to "fire-and-forget" process messages as they arrive, PGMB also provides a simpler API for that:

``` ts
const sub = await pgmb.registerFireAndForgetHandler(
	createTopicalSubscriptionParams({
		topics: ['msg-created', 'msg-updated'],
		// can expire this much quicker, because there's no
		// state to maintain
		expiryInterval: '1 minute'
	})
)
for await(const { items } of sub) {
	for(const item of items) {
		console.log('Received event', item.payload)
	}
	
	// breaking the for loop will automatically
	// unregister the subscription
	// break
	// alternatively, you can call sub.return() to
	// unregister the subscription
}
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

To flush any queued messages immediately, you can call:
``` ts
await pgmb.flush()
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

Note: table event names are lowercased automatically.

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
const sub = await pgmb.registerFireAndForgetHandler({})
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

Sometimes, it's not desirable to publish events for all mutations on a table, but only a subset of them. Or perhaps some transformations need to be applied to the data before publishing. To achieve this, PGMB allows you to override the default serialise function that determines whether to publish an event or not + have the ability to serialise the data to JSON on your own.

``` sql
REPLACE FUNCTION serialise_record_for_event(
	tabl oid,
	op TEXT, -- 'INSERT', 'UPDATE', 'DELETE'
	record RECORD,
	serialised OUT JSONB,
	emit OUT BOOLEAN
) AS $$
BEGIN
	if tabl = 'public.users'::regclass THEN
		serialised := jsonb_build_object(
			'id', record.id,
			'name', record.name
			-- note: we're omitting email & created_at from the event payload
		);
		-- don't emit events for spam users, just a demo condition
		emit := record.email NOT LIKE '%@spam.com';
		RETURN;
	END IF;
	
	-- add other table customisations here, if needed
	
	serialised := to_jsonb(record);
	emit := TRUE;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
	SECURITY INVOKER;
```

## HTTP SSE Subscriptions

PGMB makes it super easy to create HTTP SSE (Server Sent Events) endpoints that stream events to clients in real-time. PGMB also supports efficient resumption of SSE streams via the standard `Last-Event-ID` header, as events & their mappings are stored durably in Postgres.

``` ts
import express from 'express'

const app = express()

const handler = createSSERequestHandler.call(pgmb, {
	// obtain subscription parameters based on the request,
	// can throw errors if the request is invalid, unauthenticated
	// or any other checks fail.
	getSubscriptionOpts: req => (
		// we'll listen to user-updated events for the user ID
		// specified in the query param
		createTopicalSubscriptionParams({
			topics: ['user-updated'],
			// the old row is stored in metadata->'old'
			additionalFilters: { id: "e.metadata->'old'->>'id'" },
			additionalParams: { id: req.query.id },
		})
	),
	// upon reconnection, we'll replay at most 1000 events
	maxReplayEvents: 1000,
	// we'll only allow replaying events that are at most 5 minutes old
	maxReplayIntervalMs: 5 * 60 * 1000,
})

app.get('/sse', handler)
app.listen(8000)
```

Now, on the client side, you can connect to this SSE endpoint as follows:

``` js
const evtSource = new EventSource('/sse?id=123')

evtSource.addEventListener('user-updated', function(event) {
	const data = JSON.parse(event.data)
	console.log('User updated:', data)
})
```

Note: in case an assertion with the "last-event-id" fails, PGMB will automatically close the SSE connection with a 204, which tells the client to not retry the connection.

## Webhook Subscriptions

PGMB also supports HTTP webhooks for events, with retry logic built-in.

``` ts
const pgmb = new PgmbClient({
	webhookHandlerOpts: {
		retryOpts: {
			// if a webhook failes, retry it after
			// 1 minute, then after 5 minutes, then after 15 minutes
			retriesS: [60, 5 * 60, 15 * 60]
		},
		// timeout each webhook after 5 seconds
		timeoutMs: 5000,
		// headers added to each webhook request
		headers: {
			// add a custom user-agent header
			'user-agent': 'my-service-client/1.0.0'
		}
	},
	// this function is called to obtain webhook URLs
	// for a given set of PGMB subscription IDs. These
	// subscription IDs may have 0 or more webhook URLs
	// associated with them.
	async getWebhookInfo(subIds) {
		// need to have a table that maps pgmb_sub_id to
		// webhook URL & a unique identifier for the webhook
		// subscription.
		// The PGMB subscription ID can map to multiple webhook
		// subscriptions.
		// This is just an example, adapt as per your schema.
		const { rows } = await pgmb.client.query(
			`SELECT id, url, pgmb_sub_id as "subId"
			FROM webhook_subscriptions
			WHERE pgmb_sub_id = ANY($1)`,
			[subIds]
		)
		return rows.reduce((acc, row) => {
			acc[row.subId] ||= []
			acc[row.subId].push({ url: row.url })
			return acc
		}, {} as Record<string, WebhookInfo[]>)
	}
})
await pgmb.init()

// creating a webhook subscription,
// just as a more advanced example, we're inserting 
// the subscription & webhook in a single transaction
const cl = await pool.connect()
await cl.query('BEGIN')

// create a webhook subscription for all "order-placed" events
// (of course any other conditions can be used here too)
const sub = await pgmb.assertSubscription(
	createTopicalSubscriptionParams({
		topics: ['order-placed']
	}),
	cl
)

// insert a webhook for this subscription
await cl.query(
	`INSERT INTO webhook_subscriptions (pgmb_sub_id, url)
	VALUES ($1, $2)`,
	[sub.id, 'https://webhook.site/12345']
)

await cl.query('END')
cl.release()
```

The above setup will ensure that any webhooks associated with a subscription are called whenever events are published to that subscription. In case of failures, the webhooks will be retried based on the provided retry options.
Each webhook request is further paired with a `idempotency-key` header, which remains constant across retries for the same event. This allows webhook handlers to be idempotent, and safely handle retries without worrying about duplicate processing.

If you need to override the serialisation of webhook payloads, you can provide a custom function while creating the `PgmbClient`:

``` ts
const pgmb = new PgmbClient({
	webhookHandlerOpts: {
		serialiseEvent(ev) {
			return {
				body: JSON.stringify(ev),
				contentType: 'application/json'
			}
		},
		...
	},
	...
})
```

## Configuring Knobs

PGMB provides a number of configuration options to tune its behaviour (eg. how often to poll for events, read events, expire subscriptions, etc.). These can be configured via relevant env vars too, the names for which can be found [here](src/client.ts#L105)
``` ts
// poll every 500ms
process.env.PGMB_READ_EVENTS_INTERVAL_MS = '500' // default: 1000
const pgmb = new PgmbClient(opts)
```

## Production Considerations

PGMB relies on 2 functions that need to be run periodically & only once globally to ensure smooth operation, i.e.
1. `poll_for_events()` -- finds unread events & assigns them to relevant subscriptions.
	It's okay if this runs simultaneously in multiple processes, but that can create unnecessary contention on the `unread_events` table, which can bubble up to other tables.
2. `maintain_events_table()` -- removes old partitions & creates new partitions for the events table. It's also okay if this runs simultaneously in multiple processes, as it has advisory locks to ensure only a single process is maintaining the events table at any time, but running this too frequently can cause unnecessary overhead.

If you're only running a single instance of your service, you can simply run these functions in the same process as your PGMB client (default behaviour).
However, for larger deployments with multiple instances of your service, it's recommended to run these functions in a separate process to avoid contention, and disable polling & table maintainence:
``` ts
const pgmb = new PgmbClient({
	pollEventsIntervalMs: 0,
	tableMaintainanceMs: 0,
	...otherOpts
})
```

Something like [pg_cron](https://github.com/citusdata/pg_cron) is a good option.

## General Notes

- **Does the client automatically reconnect on errors & temporary network issues?**
	- If using a `pg.Pool` as the client, yes -- the pool will automatically handle reconnections.
	- If using a single `pg.Client`, then no -- you'll have to handle reconnections on your own.
- **What happens if we accidentally start multiple instances of the same PGMB client with the same `groupId`?**
	PGMB uses advisory locks to ensure that only a single instance of a subscription is active. If multiple instances are started with the same `groupId`, only one will be active, and the others will wait for the lock to be released. This ensures that there are no duplicate deliveries of events.
- **Upgrade from 0.1.x to 0.2.x**
	`0.2.x` is a completely new implementation of PGMB, with a different architecture & design. So when upgrading from `0.1.x` to `0.2.x`, code will need to be rewritten to use the new API. However, the new schema does not interfere with the old schema, so both versions can co-exist in the same database, allowing for a gradual migration. To add 0.2.x to an existing database with 0.1.x, simply run the setup script for 0.2.x -- this will add the new tables & functions required for 0.2.x without affecting the existing 0.1.x setup.
	Practically all features from `0.1.x` can be achieved in `0.2.x`.
