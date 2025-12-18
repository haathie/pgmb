# PGMB - Architecture & SQL Docs

## Installation

The primary code is just SQL functions, that can be utilised in any Postgres database. We've added a typescript client for `NodeJS` for ease of use & typesafety, it handles all maintainance process required automatically. However of course, you can use the SQL functions directly in any language that can connect to Postgres.

In Postgres:
- Clone the repository: `git clone https://github.com/haathie/pgmb.git`
- The library must be installed in the database you want to use it in. This can be done by running the following command:
  ```sh
  psql postgres://<user>:<pass>@<host>:<port>/<db> -f sql/pgmb.sql -1
  ```
  Note: `psql` must be installed on the machine executing this command.

## Testing

1. Clone repository
2. Install dependencies: `npm i`
3. Run test container: `docker compose -f docker/test.docker-compose.yaml up -d`
4. Run tests: `npm test`

## Events

All events are pushed to a table called `pgmb.events`. This table is an insert-only log of all events that have been published. It's automatically partitioned, and requires no autovacuumming -- to maximise throughput & compute utilisation. Each event is written only once to this table, and retreived via a join.
Apart from quick dropping of old data, partitioning the events table also has more benefits:
	- As this table will be very hot for both reads & writes, the insert-only design ensures that there are no deadlocks or contention on the table.
		- Caveat: we do have a trigger on the table to insert into the `unread_events` table, which is an insert/delete table -- but as this table is a single column table with just event IDs, any autovacuuming contention is minimised.
	- The partitioning also leads to the most recent events remaining in Postgres's working set, allowing for fast operations.
	- Keeping data in constant sized partitions also ensures that we get predictable table sizes.
	
At the time of writing, the events table has:
- `id` (pgmb.event_id): the unique chronological ID of the event, partitioned by this column.
- `topic` (text): the topic of the event, has no special meaning to PGMB, it's just a way to categorise events.
- `payload` (jsonb): the payload of the event, can be any JSON object.
- `metadata` (jsonb): metadata about the event, can be any JSON object.
- `subscription_id`: if an event has to be directly sent to a single subscription, this column is set to that subscription's ID. This is useful for direct messaging scenarios.

### Maintaining the Events Table

The `pgmb.events` table is partitioned by the ID property, which is a chronological ID. To maintain the partitions, a background job is required to create new partitions & drop old ones. This is done via:
``` sql
SELECT pgmb.maintain_events_table();
```

### Configuring Event Retention & Partitioning

Configuration for PGMB is stored in the `pgmb.config` table. You can set the event retention period by updating the `partition_retention_period` key:
```sql
UPDATE pgmb.config
SET value = '7 days'
WHERE key = 'partition_retention_period';
```

To change the partition interval (i.e. how often new partitions are created), update the `partition_interval` key:
```sql
UPDATE pgmb.config
SET value = '1 day'
WHERE key = 'partition_interval';

-- also set how many future partitions to maintain
-- this must be greater than or equal to the partition interval
UPDATE pgmb.config
SET value = '14 days'
WHERE key = 'future_intervals_to_create';
```

Note: any change to partition interval is automatically handled by the `maintain_events_table` function, so you can safely change it on the fly.

The default config can be found [in the SQL DDL](/sql/pgmb.sql#L40)

### Event ID Generation

All event IDs are generated using the `pgmb.create_event_id` function. Each ID is a sequentially generated ID that is built from the consumption time and some randomness, prefixed by `pm`. I.e. `pm<13 hex digits of consumption ts><9 hex digits of random>`.
When sending/publishing events in bulk, the `send` and `publish` functions will internally create a "starting" random number, and then increment it for each message sent/published in the batch -- this ensures that the IDs are all unique, and we retain the order of messages in the batch.

## Subscriptions

A subscription defines a set of conditions that determine which events the subscriber is interested in. These are stored in `pgmb.subscriptions` table.

A subscription's conditions are specified by the "conditions_sql" and "params" columns:
- `conditions_sql` is a SQL expression that is evaluated for each event, and if it evaluates truthfully, then the event is considered to have matched the subscription. It references two aliases:
	- `e` which refers to the event being evaluated (of type `pgmb.event`)
	- `s` which refers to the subscription being evaluated (of type `pgmb.subscription`)
- `params` is a JSONB object that can be used to parameterise "conditions_sql". The fewer unique "conditions_sql" queries there are, the more efficient the matching process will be.

To make this operation efficient & not bloat event-subscription matching to N^2 complexity, a GIN index is created on the "params" column, allowing for and the "conditions_sql" should be written in a way that can leverage this index. As the subscriptions table isn't expected to be written to anywhere as often as the events table, the GIN index is also setup with "fastupdate" disabled, to improve event-subscription matching performance.

As an example, this subscription matches any event where the event's "topic" matches the subscription's "topic" parameter.
```sql
INSERT INTO pgmb.subscriptions (group_id, conditions_sql, params)
VALUES (
	'my-unique-group-id',
	's.params @> jsonb_build_object(''topic'', e.topic)',
	jsonb_build_object('topic', 'user.created')
);
```

To optimise the separate process/instance that'll be required to actually consume & deliver events to subscribers, we provide a way to group subscriptions together, to allow reading events for multiple subscriptions in a single query.
`group_id` column specifies this grouping in the subscriptions table, a group owns a set of subscriptions, and each running instance must use a unique "group_id" to read events for that group of subscriptions.

### Dispatching Events to Subscribers

First, as we may have thousands of subscriptions, uniquely matching events to subscriptions can be quite expensive. So, to minimise the number of unique SQL queries that have to run to match events to subscriptions, PGMB automatically groups subscriptions that have the same "conditions_sql" together. This means that if you have multiple subscriptions with the same "conditions_sql", they will be evaluated together, reducing the number of matching operations required.

Furthermore, each subscription is uniquely constrained on (group, conditions_sql, params). This means that if multiple consumers require listening to the same set of events, they share the same underlying subscription, thereby reducing the number of event dispatching required.
For eg. if you have 100 users listening to live updates on a chat room, rather than creating 100 unique subscriptions, PGMB automatically enforces a single subscription for that chat room, and all 100 users will receive events from that single subscription.

Let's see how this works, the read process is broken up into two steps `poll_for_events` & `read_next_events`.

#### Poll for Events

The `poll_for_events` function is used to poll for new events for all groups. This runs only once at any given time. Simultaneous calls to this fn are automatically handled via advisory locks. Regular calls to this function are required to ensure that new events are matched to subscriptions.

Before we read events, we need to prepare the `poll_for_events` function with the unique set of `conditions_sql` queries that need to be evaluated. This is done automatically whenever a subscription is added/removed/updated, by internally calling the `prepare_poll_for_events` function. This is only done whenever a new unique `conditions_sql` is added/removed, so if multiple subscriptions share the same `conditions_sql`, this function call is skipped.

Now that our `poll_for_events` function is prepared, we can call it to poll for new events:
1. Whenever an event is inserted to the `events` table, a trigger inserts them into the `unread_events` table. This table contains all events that have not yet been dispatched to any subscription.
2. The `poll_for_events` function scans the `unread_events` table for new events that are ready to be consumed (i.e. `id < create_event_id(NOW(), 0)`), and deletes them from the `unread_events` table (to mark them as being processed).
	- Events are polled with with a specified "chunk_size" (to limit the number of events processed in a single call). This chunk_size can also be configured in the `pgmb.config` table via the `poll_chunk_size` key.
	- As `unread_events` will be a hot table too & it has frequent deletes too, it's configured with aggressive autovacuum settings to ensure that bloat is minimised. Also since unread_events is a single column table with just event IDs, the size of this table is quite small, allowing autovacuum to keep up with the deletes.
3. The prepared match queries are then executed against the polled events, and any matching subscriptions are recorded in the `subscription_events` table, which stores the mapping of events to subscriptions.

Subscriptions can also be automatically expired after a certain period of inactivity (useful for temporary consumers like HTTP SSE connections).

#### Read Next Events

Once events are polled and matched to subscriptions, the group can read the next events for their subscriptions using the `read_next_events` function.

This is quite straightforward, the `read_next_events` function is called with the `group_id` of the group reading events, a `cursor`, and a `chunk_size` to limit the number of events read in a single call.
	- If the `cursor` is NULL, it reads from the last stored cursor for the group, found in the `subscription_groups` table.
	- The function reads the next set of events for all subscriptions in the group, up to the specified `chunk_size` from the `subscription_events` table.
	- Subscription IDs are grouped by event, as that returns a more compact result set.
	- The function returns the events, along with the latest cursor position. Note: the cursor isn't updated just yet, and it is up to the caller to update the cursor after processing the events.

A call to this function (with default `peek := FALSE` param), acquires an advisory lock for the group, ensuring that only one instance is reading events for the group at any given time. This ensures that multiple instances don't read the same events simultaneously.
It is up to the caller to update the cursor after processing the events & release the lock, this is done via:
``` sql
SELECT pgmb.set_group_cursor(
 gid := '<group-id>',
 cursor := '<new-cursor>',
 release_lock := TRUE
);
```

## Retrying Events

Retrying isnt native to PGMB's architecture, but easily emerges from it.
Essentially if a handler of a subscription fails to process an event, it can re-insert the event IDs into a special retry event, which is then dispatched to the same subscription again.

A keen eye may notice that all handlers of the subscription will receive the retried event, so to avoid this, the retry event can include an identifier for the handler that is retrying the event, so that other handlers can ignore it. The typescript client implements this.

Simple typescript example:
``` ts
const handler: IEventHandler = async({ items }, { subscriptionId }) => {
	items = items.filter(i => (
		// ignore retry events for other handlers
		i.topic !== 'pgmb-retry' || (i.payload.handlerName === 'someWork')
	))
	try {
		await someWork(items)
	} catch(err) {
			console.error('error in event:', err)
			await pool.query(
				`insert into pgmb.events(id, topic, payload, subscription_id)
				values (
					pgmb.create_event_id(NOW() + $1::interval, 0),
					$2, $3, $4
				)`,
				[
					'1 minute',  // retry after 1 minute
					'pgmb-retry',
					{
						ids: items.map(i => i.id),
						// can be incremented to stop further retries
						retryNumber: 1,
						handlerName: 'someWork'
					}
					subscriptionId
				]
			)
	}
}
```

The full implementation of the inbuilt retry mechanism in the typescript client can be found [here](/src/retry-handler.ts) and in [the client](/src/client.ts). It implements automatic retries with a customisable backoff strategy.

## Reliable Event Delivery & Processing

PGMB's architecture can be leveraged to build reliable event delivery & processing systems -- that gives us `at-least-once` delivery. Let's see how this is achieved by the typescript client:
1. Let's say we've a few listeners for some subscriptions:
``` ts
const params = createTopicalSubscriptionParams({
	topics: ['msg-created', 'msg-updated'],
	expiryInterval: '15 minutes'
})
const l1 = await pgmb.registerReliableHandler(
	params,
	async() => {
		// Do some work
	}
)
const l1 = await pgmb.registerReliableHandler(
	params,
	async() => {
		// Do some more work
	}
)

// let's also assume the same subscription's being listened
// in a fire-and-forget manner
const l3 = await pgmb.registerFireAndForgetHandler(params)
```

Now, whenever an event is published with topic `msg-created` or `msg-updated`, all three listeners will receive the event.
2. `l3` is a fire-and-forget listener, so it just receives the event and processes it, we don't expect any contention from this. `l1` & `l2` are reliable listeners, so they need to ensure that the event is processed successfully, this may take multiple seconds too.
3. The typescript client acquires a dedicated session, and calls in `read_next_events` for the group every specified interval. Once all handlers have processed the events successfully, the cursor is updated for all handlers in the group via the same session, releasing the acquired advisory lock on `read_next_events`.
4. If `l1` fails, that entire batch is failed, we start reading events from the last cursor again, ensuring that no events are lost. This process will continue indefinitely until all events are processed successfully. Of course, this can lead to tons of duplicate events being sent to all handlers, and overall can be quite inefficient.
Thus, it's recommended that to avoid poison pills, wrap handlers in a try-catch block or provide `retryOpts` when registering a reliable sub. Eg.
``` ts
const l1 = await pgmb.registerReliableHandler(
	{
		...params,
		// unique name for the handler, in a subscription.
		// Allows us to filter out retries meant for other handlers
		name: 's1',
		retryOpts: {
			// retry after 1 minute, then after 5 minutes
			retriesS: [1*60, 5*60] 
		}
	},
	async({ items }) => {
		// Do some work
	}
)
```
5. Let's say `l2` succeeds, but `l1` is still processing. This will block the next batch of events from being processed by both `l2` & `l3`, as PGMB polls for events in a group & to ensure reliable processing, it waits for all handlers to finish processing before moving the cursor forward.
So, to avoid a single slow handler blocking all other handlers, PGMB reads ahead and waits for the slowest handler to finish processing, while other handlers can continue processing new events.
It keeps the latest read cursor in memory, and whenever the handlers finishes processing for a particular batch, its "checkpoint" is reached, and the cursor for that batch is updated in the database. Upon the completion of all checkpoints, the advisory lock on `read_next_events` as well as the acquired session for reading is released.
To avoid memory bloat, the number of read-ahead batches is limited to a configurable number (default: 10), so if there are too many slow handlers, the faster handlers may have to wait for the slow ones to finish before processing new events. Therefore, it's recommended to give handlers a timeout, to avoid them being slow indefinitely.

## Uninstalling PGMB

To uninstall PGMB, you can simply drop the `pgmb` schema from your database. This will remove all tables, functions, and other objects created by PGMB.
```sql
DROP SCHEMA pgmb CASCADE;
```

## Upgrading PGMB

TODO

### Generating Upgrades

1. Add the old SQL file to the `sql/` directory, e.g. `sql/pgmb_0.1.0.sql` (let's assume current version is `0.1.1`).
2. Download [APGDiff](https://www.apgdiff.com/download.php)
3. Run the following command to generate the upgrade SQL file:
   ```sh
   java -jar apgdiff-2.4.jar sql/pgmb-0.1.0.sql sql/pgmb.sql > sql/upgrade--0.1.0--0.1.1.sql
   ```
4. Review & push the generated file to the repository. 
  - Reinstall `pgmb` with the old version (`psql <pg-url> -f sql/pgmb_0.1.0.sql -1`)
  - Create some queues & exchanges to test the upgrade, possibly run the benchmark scripts
  - Simultaneously, run the upgrade SQL file (`psql <pg-url> -f sql/upgrade--0.1.0--0.1.1.sql -1`)
  - Run tests to verify the upgrade works as expected (`npm test`).
