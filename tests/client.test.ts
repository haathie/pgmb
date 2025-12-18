import assert from 'assert'
import { Chance } from 'chance'
import type { ErrorEvent } from 'eventsource'
import { EventSource } from 'eventsource'
import { readFile } from 'fs/promises'
import type { IncomingHttpHeaders, Server, ServerResponse } from 'node:http'
import { createServer } from 'node:http'
import {
	after,
	afterEach,
	before,
	beforeEach,
	describe,
	it,
	mock,
} from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { Pool, type PoolClient } from 'pg'
import { pino } from 'pino'
import type {
	IEvent,
	IEventHandler,	ITableMutationEventData,
	WebhookInfo } from '../src/index.ts'
import {
	createRetryHandler,
	createSSERequestHandler,
	createTopicalSubscriptionParams,
	type IReadEvent,
	PgmbClient
} from '../src/index.ts'
import {
	maintainEventsTable,
	pollForEvents,
	removeExpiredSubscriptions,
	scheduleEventRetry,
	writeScheduledEvents,
} from '../src/queries.ts'

const LOGGER = pino({ level: 'trace' })
const CHANCE = new Chance()

type TestEventData = {
	topic: 'test-topic'
	payload: { data: number }
} | {
	topic: 'test-topic-1'
	payload: { key: string }
} | ITableMutationEventData<
	{
		id: number
		data: string
	},
	'public.test_table'
>

type ITestEvent = IEvent<TestEventData>

describe('PGMB Client Tests', () => {
	const pool = new Pool({ connectionString: process.env.PG_URI, max: 20 })
	const webhookInfos: { [subId: string]: WebhookInfo[] } = {}

	let client: PgmbClient<TestEventData>
	let groupId: string

	before(async() => {
		await pool.query('DROP SCHEMA IF EXISTS pgmb CASCADE;')

		const sql = await readFile('./sql/pgmb.sql', 'utf-8')
		await pool.query(sql)
		// swallow errors
		pool.on('error', () => {})
	})

	after(async() => {
		await pool.end()
	})

	beforeEach(async() => {
		groupId = `grp${Math.random().toString(36).substring(2, 15)}`

		client = new PgmbClient({
			client: pool,
			logger: LOGGER,
			poll: true,
			groupId,
			sleepDurationMs: 250,
			subscriptionMaintenanceMs: 1000,
			maxActiveCheckpoints: 3,
			getWebhookInfo: () => webhookInfos,
			webhookHandlerOpts: {
				retryOpts: { retriesS: [1, 2] },
				timeoutMs: 1_000
			}
		})
		await client.init()
	})

	afterEach(async() => {
		await client.end()
	})

	it('should receive events on multiple subs', async() => {
		// we'll create 3 subs, 2 identical, 1 different
		// to check if de-duping works correctly on same conditions & params
		const sub1 = await client.registerFireAndForgetSubscription({})
		const sub2 = await client.registerFireAndForgetSubscription({})
		const sub3 = await client.registerFireAndForgetSubscription({ params: { a: 1 } })

		// check de-duping happened correctly
		assert.equal(sub1.id, sub2.id)
		assert.notEqual(sub1.id, sub3.id)

		const sub2RecvTask = Array.fromAsync(sub2)
		const sub3RecvTask = Array.fromAsync(sub3)

		const inserted = await insertEvent(pool)

		const recv: unknown[] = []

		for await (const { items } of sub1) {
			recv.push(...items)
			if(recv.length === 1) {
				await insertEvent(pool)
			}

			if(recv.length >= 2) {
				break
			}
		}

		assert.partialDeepStrictEqual(recv[0], inserted)

		await sub2?.return()
		await sub3?.return()

		const sub2Recv = await sub2RecvTask
		const sub3Recv = await sub3RecvTask

		// other subs shouldve received both events
		assert.equal(sub2Recv.flatMap((s) => s.items).length, 2)
		assert.equal(sub3Recv.flatMap((s) => s.items).length, 2)

		// check old events aren't replayed, on a new subscription
		const sub4 = await client.registerFireAndForgetSubscription({ params: { a: 4 } })
		const rslt = await Promise.race([sub4.next(), setTimeout(250)])
		assert.equal(rslt, undefined)
		sub4.return()
	})

	it('should remove expired subs', async() => {
		const sub = await client
			.registerFireAndForgetSubscription({ expiryInterval: '1 second' })
		sub.return()

		await setTimeout(1500)

		const {
			rows: [{ count }],
		} = await pool.query<{ count: string }>(
			'select count(*) as count from pgmb.subscriptions where id = $1',
			[sub.id],
		)
		assert.equal(count, '0')
	})

	it('should handle disconnections when reading changes', async() => {
		const sub = await client.registerFireAndForgetSubscription({})
		const evs = Array.fromAsync(sub)

		await insertEvent(pool)
		await setTimeout(500)
		// disconnect all backends
		await pool.query(
			'SELECT pg_terminate_backend(pid) FROM pg_stat_activity'
		).catch(() => {})
		await setTimeout(50)
		await insertEvent(pool)

		await setTimeout(1000)

		await sub.return()
		const received = await evs
		assert.equal(
			received.flatMap(r => r.items).length,
			2
		)
	})

	it('should not read uncommitted events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')
		const ins = await insertEvent(c1)

		const sub = await client.registerFireAndForgetSubscription({})
		const received: unknown[] = []
		const readPromise = (async() => {
			for await (const evs of sub) {
				received.push(...evs.items)
				break
			}
		})()

		await setTimeout(250)

		assert.equal(received.length, 0)

		await c1.query('COMMIT;')
		c1.release()

		await readPromise

		assert.partialDeepStrictEqual(received, [ins])
	})

	it('should handle concurrent subscription writes', async() => {
		await client.registerFireAndForgetSubscription({})
		await client.readChanges()
		await insertEvent(pool)
		const [changeCount1] = await Promise.all([
			client.readChanges(),
			pollForEvents.run(undefined, pool),
		])
		const changeCount2 = await client.readChanges()
		assert.equal(changeCount1 + changeCount2, 1)
	})

	it('should receive events from concurrent transactions', async() => {
		const eventcount = 500
		const c1 = await pool.connect()
		await c1.query('BEGIN;')

		const events: unknown[] = []
		const sub = await client.registerFireAndForgetSubscription({})
		const readPromise = (async() => {
			for await (const evs of sub) {
				events.push(...evs.items)
				if(events.length >= eventcount) {
					break
				}
			}
		})()

		const eventsWritten: unknown[] = []

		await Promise.all([
			readPromise,
			(async() => {
				for(let i = 0; i < eventcount; i++) {
					eventsWritten.push(await insertEvent(c1))
				}

				await c1.query('COMMIT;')
			})()
		])

		assert.equal(events.length, eventcount)

		c1.release()

		assert.partialDeepStrictEqual(events, eventsWritten)
	})

	it('should handle concurrent changes', async() => {
		await client.end()

		const initialPartCount = await getEventsPartitionCount()

		await Promise.all([
			Array.from({ length: 5 }).map(() => insertEvent(pool)),
			pollForEvents.run(undefined, pool),
			removeExpiredSubscriptions.run(
				{ groupId: client.groupId, activeIds: [] },
				pool,
			),
			await pool.query(
				`select pgmb.maintain_events_table(
					current_ts := NOW()
						+ pgmb.get_config_value('partition_interval')::interval
				);`,
			),
		])

		// check partitions exist
		const {
			rows: [{ count }],
		} = await pool.query<{ count: string }>(
			`SELECT
				count(*) as count
			FROM pg_catalog.pg_inherits
			WHERE inhparent = 'pgmb.events'::regclass;`,
		)
		// we'd removed 1 old partition by executing maintainence w a future
		// timestamp, which would've removed 1 old partition and created 1 new one
		// since by default we retain 2 oldest partitions, we should be 1 ahead
		assert.equal(+count, initialPartCount + 1)

		await client.init()
	})

	it('should handle partition interval changes', async() => {
		await client.end()

		const initialPartCount = await getEventsPartitionCount()
		await pool.query(
			"UPDATE pgmb.config SET value = '1 minute' WHERE id = 'partition_interval';"
		)
		await maintainEventsTable.run(undefined, pool)

		const initialPartCount2 = await getEventsPartitionCount()
		assert.equal(initialPartCount2, initialPartCount)

		await pool.query(
			`UPDATE pgmb.config
			SET value = (value::interval + '30 minutes' + '2 minutes'::interval)::text
			WHERE id = 'future_intervals_to_create';`
		)
		await maintainEventsTable.run(undefined, pool)

		// check inserting events still works
		await insertEvent(pool)

		await pool.query(
			"UPDATE pgmb.config SET value = '1 day' WHERE id = 'partition_interval';"
			+ `UPDATE pgmb.config SET value = '2 days'
			WHERE id = 'future_intervals_to_create';`
		)

		await maintainEventsTable.run(undefined, pool)

		// check inserting events still works
		await writeScheduledEvents.run(
			{
				ts: [new Date(Date.now() + 24 * 60 * 60 * 1000)],
				topics: ['test-topic'],
				payloads: [{ data: 123 }],
				metadatas: [{}],
			},
			pool
		)
	})

	it('should handle multiple clients w the same groupId', async() => {
		const evCount = 20

		const addClients = await Promise.all(
			Array.from({ length: 2 }).map(async(_, i) => {
				const pgmb = new PgmbClient<TestEventData>({
					groupId: client.groupId,
					logger: LOGGER.child({ dup: i + 1 }),
					client: pool,
					poll: true,
					sleepDurationMs: 100
				})
				await pgmb.init()
				return pgmb
			})
		)

		const allClients = [client, ...addClients]
		let processed = 0
		await Promise.all(
			allClients.map((c) => (
				c.registerReliableSubscription(
					createTopicalSubscriptionParams<TestEventData>({
						topics: ['test-topic-1']
					}),
					async({ items }) => {
						await setTimeout(Math.random() * 250)
						processed += items.length
					}
				)
			))
		)

		for(let i = 0; i < evCount;i++) {
			await client.publish(
				[{ topic: 'test-topic-1', payload: { key: i.toString() } }]
			)
			await setTimeout(50)
		}

		while(processed < evCount) {
			await setTimeout(100)
		}

		await setTimeout(1000)

		await Promise.all(addClients.map(cl => cl.end()))

		assert.equal(processed, evCount)
	})

	it('should not read future events', async() => {
		const DELAY_MS = 1000

		const topic = 'scheduled-event'
		const sub = await client.registerFireAndForgetSubscription(
			createTopicalSubscriptionParams({ topics: [topic] })
		)

		const [prow, frow] = await writeScheduledEvents.run(
			{
				ts: [new Date(), new Date(Date.now() + DELAY_MS)],
				topics: [topic, topic],
				payloads: [{ a: 1 }, { a: 2 }],
				metadatas: [{}, {}],
			},
			pool,
		)

		const nxt = await sub.next()
		assert(!nxt.done)
		assert.equal(nxt.value.items.length, 1)
		assert.partialDeepStrictEqual(nxt.value, { items: [prow] })

		await setTimeout(DELAY_MS)

		const nxt2 = await sub.next()
		assert(!nxt2.done)
		assert.equal(nxt2.value.items.length, 1)
		assert.partialDeepStrictEqual(nxt2.value, { items: [frow] })
	})

	it('should not read duplicate events', async() => {
		const writerCount = 10
		const eventsPerWriter = 300
		const eventsToWrite = writerCount * eventsPerWriter

		const events: { payload: unknown }[] = []
		const sub = await client.registerFireAndForgetSubscription({})
		const readPromise = (async() => {
			for await (const evs of sub) {
				events.push(...evs.items)
				if(events.length >= eventsToWrite) {
					break
				}
			}
		})()

		const eventsWritten: { payload: unknown }[] = []
		const task = Promise.all(
			Array.from({ length: writerCount }).map(async() => {
				const c = await pool.connect()
				let state: 'in-tx' | 'out-tx' = 'out-tx'
				for(let i = 0; i < eventsPerWriter; i++) {
					if(state === 'out-tx' && Math.random() < 0.1) {
						await c.query('BEGIN;')
						state = 'in-tx'
					}

					eventsWritten.push(await insertEvent(c))

					if(state === 'in-tx' && Math.random() < 0.1) {
						await c.query('COMMIT;')
						state = 'out-tx'

						await setTimeout(Math.floor(Math.random() * 30))
					}
				}

				if(state === 'in-tx') {
					await c.query('COMMIT;')
				}

				c.release()
			}),
		)

		await readPromise

		// ensure all events got read
		for(const ev of events) {
			assert.ok(
				eventsWritten.some(
					(e) => JSON.stringify(e.payload) === JSON.stringify(ev.payload),
				),
			)
		}

		// ensure no duplicate events
		assert.equal(events.length, eventsToWrite)

		await task
	})

	it('should handle large numbers of events & subs', async() => {
		// ending client to avoid simultaneous pollers
		await client.end()

		// remove existing subs
		await pool.query('truncate pgmb.subscriptions;')

		const EVENT_COUNT = 10_000
		const SUB_PER_TYPE_COUNT = 1_000
		const SUB_TYPES = 4
		const TOTAL_SUB_COUNT = SUB_PER_TYPE_COUNT * SUB_TYPES

		for(let i = 0; i < SUB_TYPES; i++) {
			const k = `key${i}`
			await pool.query<{ id: string }>(
				`insert into pgmb.subscriptions (group_id, conditions_sql, params)
				select * from unnest($1::text[], $2::text[], $3::jsonb[])
				 as t(group_id, conditions_sql, params)
				returning id`,
				[
					Array.from({ length: SUB_PER_TYPE_COUNT }).map(() => groupId),
					Array.from({ length: SUB_PER_TYPE_COUNT }).map(
						() => `s.params @> jsonb_build_object(\'${k}\', e.payload->>\'key\')`,
					),
					Array.from({ length: SUB_PER_TYPE_COUNT }).map((_, i) => ({
						[k]: i.toString(),
					})),
				],
			)
		}

		await client.publish(
			Array.from({ length: EVENT_COUNT }).map((_, i) => ({
				topic: 'test-topic-1',
				payload: { key: (i % SUB_PER_TYPE_COUNT).toString() },
			}))
		)

		console.log('Inserted events and subs, starting poll...')

		const now = Date.now()
		const [{ count }] = await pollForEvents.run(undefined, pool)

		const tt = Date.now() - now
		console.log(
			`Processed ${count} events for ${TOTAL_SUB_COUNT} subs in ${tt}ms`,
		)

		// ensure it took less than 1.5 seconds
		assert(tt <= 1500, `Took too long: ${tt}ms`)

		assert.equal(count, SUB_TYPES * EVENT_COUNT)

		await pool.query('truncate pgmb.subscription_events;')

		await client.init()
	})

	it('should update poll fn when conditions_sql uniquely changed', async() => {
		const cond = "e.topic = s.params->>'topic'"
		const fnData0 = await getPollFnData()
		await client.registerFireAndForgetSubscription({
			conditionsSql: cond,
			params: { topic: 'test1' },
		})
		const fnData1 = await getPollFnData()
		assert.notEqual(fnData0.prosrc, fnData1.prosrc)

		const sub2 = await client.registerFireAndForgetSubscription({
			conditionsSql: cond,
			params: { topic: 'test2' },
		})
		const fnData2 = await getPollFnData()
		assert.deepEqual(fnData1.prosrc, fnData2.prosrc)
		await sub2.return?.()

		await setTimeout(500)

		const fnData3 = await getPollFnData()
		assert.deepEqual(fnData2.prosrc, fnData3.prosrc)
	})

	it('should concurrently update poll fn', async() => {
		const conds = [
			"e.topic = s.params->>'topic'",
			"e.payload->>'data' = s.params->>'data'",
			"(e.payload->>'value')::int > (s.params->>'min_value')::int",
		]
		await Promise.all([
			client.readChanges(),
			...conds.map((cond) => client.registerFireAndForgetSubscription({ conditionsSql: cond }),
			),
		])

		await client.readChanges()

		const { prosrc } = await getPollFnData()
		for(const cond of conds) {
			assert.ok(prosrc.includes(cond))
		}
	})

	it('should fail to create subscription with invalid conditions SQL', async() => {
		await assert.rejects(
			() => client.registerFireAndForgetSubscription({ conditionsSql: 'INVALID SQL' })
		)
	})

	it('should re-enqueue event for subscription', async() => {
		const sub1 = await client.registerFireAndForgetSubscription({ conditionsSql: 'TRUE' })
		// control subscription
		const sub2 = await client.registerFireAndForgetSubscription({
			conditionsSql: 'TRUE AND TRUE',
		})

		await insertEvent(pool)

		const [{ done, value }] = await Promise.all([sub1.next(), sub2.next()])
		assert(!done)
		assert.equal(value.items.length, 1)

		const now = Date.now()
		const reSubId = sub1.id
		await scheduleEventRetry.run(
			{
				ids: [value.items[0].id],
				retryNumber: 1,
				subscriptionId: reSubId,
				delayInterval: '1 second',
			},
			pool,
		)

		const e2 = await Promise.race([sub1.next(), sub2.next()])
		assert(!e2.done)

		// shouldve come after at least 1 second
		assert.ok(Date.now() - now >= 1000)

		assert.equal(e2.value.items.length, 1)
		assert.partialDeepStrictEqual(e2.value.items, [
			{ subscriptionIds: [sub1.id] },
		])
	})

	it('should match subscriptions', async() => {
		const noSub = await client.registerFireAndForgetSubscription({
			conditionsSql: "e.payload->>'non_exist' IS NOT NULL",
		})

		const noSubItems = Array.fromAsync(noSub)

		const sub1 = await client.registerFireAndForgetSubscription({
			conditionsSql: "e.payload->'data' > s.params->'min'",
			params: { min: 0.5 },
		})
		const sub2 = await client.registerFireAndForgetSubscription({
			conditionsSql: "e.payload->'data' > s.params->'min'",
			params: { min: 0 },
		})

		await client.publish(
			[
				{ topic: 'test-topic', payload: { data: 0.7 } },
				{ topic: 'test-topic', payload: { data: 0.3 } },
			]
		)

		// 0.3 !> 0.5, but 0.3 > 0 -- so matched only by sub2
		const sub1Nxt = await sub1.next()
		assert(!sub1Nxt.done)
		assert.equal(sub1Nxt.value.items.length, 1)

		// 0.7 > 0.5, and 0.7 > 0 -- so matched by both subs
		const sub2Nxt = await sub2.next()
		assert(!sub2Nxt.done)
		assert.equal(sub2Nxt.value.items.length, 2)

		noSub.return?.()
		assert.deepEqual(await noSubItems, [])
	})

	it('should create events from table mutations', async() => {
		const sub = await client.registerFireAndForgetSubscription(
			createTopicalSubscriptionParams<TestEventData>({
				topics: [
					'public.test_table.delete',
					'public.test_table.insert',
					'public.test_table.update'
				]
			})
		)
		const events: unknown[] = []
		const task = (async() => {
			for await (const { items } of sub) {
				events.push(...items)
			}
		})()

		await pool.query(`
			DROP TABLE IF EXISTS public.test_table;
			CREATE TABLE public.test_table (
				id SERIAL PRIMARY KEY,
				data TEXT NOT NULL
			);
			SELECT pgmb.push_table_mutations('public.test_table'::regclass);
		`)

		await pool.query(`
			INSERT INTO public.test_table (data) VALUES ('hello'), ('world');
		`)
		await pool.query(
			"UPDATE public.test_table SET data = 'hello!!!' WHERE id = 1;",
		)
		await pool.query('DELETE FROM public.test_table WHERE id = 2;')

		while(!events.length) {
			await setTimeout(50)
		}

		assert.equal(events.length, 4)
		assert.partialDeepStrictEqual(events, [
			{ topic: 'public.test_table.insert', payload: { id: 1, data: 'hello' } },
			{ payload: { id: 2, data: 'world' } },
			{
				topic: 'public.test_table.update',
				payload: { data: 'hello!!!' },
				metadata: { old: { id: 1, data: 'hello' } },
			},
			{ topic: 'public.test_table.delete', payload: { id: 2 } },
		])

		// check removing subscribable works
		await pool.query(
			`SELECT pgmb.stop_table_mutations_push('public.test_table'::regclass);
			INSERT INTO public.test_table (data) VALUES ('new data');`
		)

		// ensure no new events come in
		await setTimeout(500)
		sub.return?.()

		await task
		assert.equal(events.length, 4)
	})

	it('should not update cursor if previous checkpoint still active', async() => {
		const cursor0 = await getGroupCursor()
		const sub2Fn = mock.fn<IEventHandler<TestEventData>>(async() => { })

		let resolveFirst: (() => void) | undefined

		await client.registerReliableSubscription(
			{},
			async() => {
				if(!resolveFirst) {
					return new Promise<void>((resolve) => {
						resolveFirst = resolve
					})
				}
			}
		)

		await client.registerReliableSubscription({}, sub2Fn)

		for(let i = 0; i < client.maxActiveCheckpoints + 2;i++) {
			await insertEvent(pool)
			await setTimeout(client.sleepDurationMs)
		}

		const cursor1 = await getGroupCursor()
		assert.equal(cursor0, cursor1)
		// sub2 shouldve been called maxActiveCheckpoints times
		// ensure we don't skip ahead too far
		assert.equal(sub2Fn.mock.callCount(), client.maxActiveCheckpoints)

		assert(resolveFirst)
		resolveFirst()

		await setTimeout(100)

		const cursor2 = await getGroupCursor()
		assert.notEqual(cursor2, cursor1)

		await setTimeout(500)

		// after the checkpoint got released, the next "readChanges()" call
		// would read the remaining events in 1 batch
		assert.equal(sub2Fn.mock.callCount(), client.maxActiveCheckpoints + 1)
	})

	it('should replay full batch on reliable handler failure', async() => {
		const sub2Fn = mock.fn<IEventHandler<TestEventData>>(async() => { })

		let failed = false

		await client.registerReliableSubscription(
			{},
			async() => {
				if(!failed) {
					failed = true
					throw new Error('Simulated failure')
				}
			}
		)
		await client.registerReliableSubscription({ params: { a: 1 } }, sub2Fn)

		const { id } = await insertEvent(pool)

		await setTimeout(2000)

		assert.equal(sub2Fn.mock.callCount(), 2)
		assert.partialDeepStrictEqual(
			sub2Fn.mock.calls.flatMap(c => c.arguments[0].items),
			[{ id }, { id }]
		)
	})

	it('should only update cursor after reliable handlers are done', async() => {
		const EVENT_COUNT = 150
		let handled = 0

		const subs = await Promise.all(
			Array.from({ length: 3 }).map((_, i) => {
				let active = false
				return client.registerReliableSubscription(
					{
						params: { data: i },
						conditionsSql: 's.params @> e.payload',
					},
					async({ items }, {}) => {
						assert(!active, 'Handler called concurrently!')
						if(handled < EVENT_COUNT) {
							// add more events
							pushEvents(Math.floor(Math.random() * 20) + 1)
						}

						active = true
						const ms = Math.floor(Math.random() * 500) + 100
						await setTimeout(ms)
						active = false
						handled += items.length
					},
				)
			}),
		)

		// check eph sub gets all events
		const ephSub = await client.registerFireAndForgetSubscription({})
		const ephRecvTask = Array.fromAsync(ephSub)

		await pushEvents(subs.length)

		while(handled < EVENT_COUNT) {
			await setTimeout(100)
			console.log(`Handled ${handled}/${EVENT_COUNT} events...`)
		}

		await setTimeout(1000)

		const now = Date.now()
		while(Date.now() - now < 10_000) {
			// verify cursor correctly stored
			const cursor = await getGroupCursor()

			// max cursor
			const {
				rows: [{ maxId }],
			} = await pool.query<{ maxId: string }>(
				'select max(id) as "maxId" from pgmb.subscription_events',
			)
			if(maxId === cursor) {
				break
			}

			await setTimeout(200)
		}

		ephSub.return?.()
		const ephRecv = await ephRecvTask
		assert(ephRecv.flatMap((e) => e.items).length >= EVENT_COUNT)

		async function pushEvents(count: number) {
			for(let i = 0; i < count; i++) {
				await client
					.publish([{ topic: 'test-topic', payload: { data: i % 3 } }])
			}
		}
	})

	it('should retry failed handlers', async() => {
		let failedEvents: ITestEvent[] | undefined
		let doneEvents: ITestEvent[] | undefined
		const { subscriptionId } = await client.registerReliableSubscription(
			{},
			createRetryHandler(
				{ retriesS: [1] },
				async({ items }) => {
					if(!failedEvents) {
						failedEvents = items
						throw new Error('Simulated failure')
					}

					await setTimeout(100)
					doneEvents = items
				},
			),
		)

		await Promise.all([insertEvent(pool), insertEvent(pool)])

		await setTimeout(750)

		assert.ok(failedEvents)
		// ensure we've not yet completed, retry didn't get scheduled
		// too early
		assert.deepEqual(doneEvents, undefined)

		while(!doneEvents) {
			await setTimeout(100)
		}

		assert.partialDeepStrictEqual(failedEvents, doneEvents)
		// validate another event didn't get scheduled
		const {
			rows: [{ count }],
		} = await pool.query<{ count: string }>(
			`select count(*) as count from pgmb.subscription_events
			where subscription_id = $1`,
			[subscriptionId],
		)
		// 2 events, 1 retry
		assert.equal(count, '3')
	})

	it('should not reschedule after retries exhausted', async() => {
		const handler = mock.fn<(arg: IReadEvent) => Promise<void>>(() => {
			throw new Error('Always fails')
		})
		const { subscriptionId } = await client.registerReliableSubscription(
			{},
			createRetryHandler({ retriesS: [1, 1] }, handler),
		)

		await insertEvent(pool)

		while(handler.mock.callCount() < 3) {
			await setTimeout(100)
		}

		assert.equal(handler.mock.callCount(), 3)

		// check no more retries scheduled
		const {
			rows: [{ count }],
		} = await pool.query<{ count: string }>(
			`select count(*) as count from pgmb.subscription_events
			where subscription_id = $1`,
			[subscriptionId],
		)
		// only original event + 2 retry
		assert.equal(count, '3')

		const evs = handler.mock.calls.map((c) => c.arguments[0])
		assert.deepEqual(evs[0].items.map(i => i.id), evs[2].items.map(i => i.id))
	})

	describe('SSE', () => {
		let srv: Server
		let latestSrvRes: ServerResponse
		const port: number = CHANCE.integer({ min: 10000, max: 65000 })
		const sseUrl = `http://localhost:${port}/sse`

		before(async() => {
			srv = createServer()
			await new Promise<void>((resolve) => srv.listen(port, resolve))
		})

		after(async() => {
			srv.close()
		})

		beforeEach(async() => {
			const handler = (createSSERequestHandler<TestEventData>).call(
				client,
				{ getSubscriptionOpts: () => ({})	}
			)
			srv.on('request', (req, res) => {
				latestSrvRes = res
				return handler(req, res)
			})
		})

		afterEach(() => {
			srv.removeAllListeners('request')
		})

		it('should receive events over SSE', async() => {
			const { es } = await openEs()

			const task = waitForESEvent(es)
			const { id } = await insertEvent(pool)
			const result = await task
			assert.equal(result.lastEventId, id)
			assert.ok(JSON.parse(result.data))

			// ensure a graceful close, server closes listeners
			es.close()
			await setTimeout(100)
			assert.deepEqual(client.listeners, {})
		})

		it('should handle receiving missing events over SSE', async() => {
			const { es, res } = await openEs()
			const firstEventRecv = waitForESEvent(es)
			await insertEvent(pool)
			await firstEventRecv

			// simulate an error, ask to connect after 1s.
			// By that time, we'll insert a new event
			res.write('error: oops\nretry: 250\n\n')
			res.end()

			await insertEvent(pool)

			const missingEvTask = waitForESEvent(es)
			await waitForESOpen(es)

			const ev2 = await missingEvTask
			assert.ok(ev2)
			es.close()
		})

		it('should error if last-event-id is unavailable', async() => {
			const { es } = await openEs()
			const firstEventRecv = waitForESEvent(es)
			await insertEvent(pool)
			await firstEventRecv

			// remove the subscription & consequently end the current SSE connection
			await client.removeSubscription(Object.keys(client.listeners)[0])
			// removal error
			await assert.rejects(waitForESOpen(es))
			// reconnection error
			await assert.rejects(waitForESOpen(es), (err: ErrorEvent | undefined) => {
				assert.equal(err?.code, 204)
				return true
			})
		})

		it('should handle reconnection on pgmb destruction', async() => {
			const { es } = await openEs()
			await client.registerReliableSubscription({}, () => setTimeout(500))

			await insertEvent(pool)
			await waitForESEvent(es)

			await insertEvent(pool)
			await insertEvent(pool)
			// simulate pgmb client being destroyed, while simultaneously
			// bringing up a new client to handle reconnection
			const ogEndTask = client.end()
			client = new PgmbClient<TestEventData>({
				client: pool,
				logger: LOGGER.child({ r: 1 }),
				groupId: groupId,
				poll: true,
			})
			await client.init()

			const handler = (createSSERequestHandler<TestEventData>).call(
				client,
				{ getSubscriptionOpts: () => ({})	}
			)
			srv.removeAllListeners('request')
			srv.on('request', (req, res) => {
				latestSrvRes = res
				return handler(req, res)
			})

			await waitForESEvent(es)
			es.close()

			await ogEndTask
		})

		async function openEs() {
			const es = new EventSource(sseUrl)
			await waitForESOpen(es)

			assert(latestSrvRes)
			return { es, res: latestSrvRes }
		}
	})

	describe('Webhooks', () => {

		const port = CHANCE.integer({ min: 10000, max: 65000 })
		const webhookUrl = `http://localhost:${port}/webhook`
		let responses: {
			body: string
			headers: IncomingHttpHeaders
		}[] = []
		let srv: Server
		let shouldFailNextWebhook = false

		before(async() => {
			srv = createServer(async(req, res) => {
				if(req.method !== 'POST') {
					return res.writeHead(405).end()
				}

				if(!req.url?.startsWith('/webhook')) {
					return
				}

				const bodyChunks: Uint8Array[] = []
				for await (const chunk of req) {
					bodyChunks.push(chunk)
				}

				const body = Buffer.concat(bodyChunks).toString('utf-8')
				responses.push({ body, headers: req.headers })
				if(shouldFailNextWebhook) {
					shouldFailNextWebhook = false
					return res
						.writeHead(500, { 'content-type': 'text/plain' })
						.end('unhappy')
				}

				res
					.writeHead(200, { 'content-type': 'text/plain' })
					.end('happy')
			})

			srv.listen(port)
		})

		after(async() => {
			srv.close()
		})

		beforeEach(() => {
			responses = []
		})

		it('should create webhook subscription', async() => {
			const { id: subId } = await client
				.registerFireAndForgetSubscription({ })
			webhookInfos[subId] = [{ id: '1', url: webhookUrl }]
			const ev = await insertEvent(pool)

			while(!responses.length) {
				await setTimeout(100)
			}

			const [{ headers, body }] = responses
			assert.equal(headers['content-type'], 'application/json')
			assert.ok(headers['idempotency-key'])

			assert.ok(body.includes(ev.id))
		})

		it('should retry failed webhooks', async() => {
			const { id: subId } = await client
				.registerFireAndForgetSubscription({ })
			webhookInfos[subId] = [{ id: '1', url: webhookUrl }]
			await Promise.all([insertEvent(pool), insertEvent(pool)])

			shouldFailNextWebhook = true
			while(responses.length < 2) {
				await setTimeout(100)
			}

			const [
				{ headers: h1, body: b1 },
				{ headers: h2, body: b2 }
			] = responses
			assert.equal(b1, b2)
			assert.equal(h1['idempotency-key'], h2['idempotency-key'])
		})
	})

	async function getEventsPartitionCount() {
		// get current number of partitions
		const { rows: [{ count }] } = await pool.query<{ count: string }>(
			`SELECT count(*) as count FROM pg_catalog.pg_inherits
			 WHERE inhparent = 'pgmb.events'::regclass;`,
		)
		return +count
	}

	async function getGroupCursor() {
		const {
			rows: [{ cursor }],
		} = await pool.query<{ cursor: string }>(
			`select last_read_event_id as cursor from
			pgmb.subscription_groups where id = $1`,
			[client.groupId],
		)
		return cursor
	}

	async function insertEvent(db: Pool | PoolClient) {
		const topic = 'test-topic'
		const payload = { data: Math.random() }
		const [{ id }] = await client.publish([{ topic, payload }], db)
		return { id, topic, payload }
	}

	async function getPollFnData() {
		const {
			rows: [row],
		} = await pool.query<{ prosrc: string }>(
			`select * from pg_proc
			where proname = 'poll_for_events'
			and pronamespace = 'pgmb'::regnamespace`
		)
		assert(row)
		return row
	}
})

function waitForESOpen(es: EventSource) {
	return new Promise<void>((resolve, reject) => {
		es.onopen = () => {
			console.log('SSE connection opened')
			resolve()
		}

		es.onerror = (err) => {
			console.error('SSE connection error', err)
			reject(err)
		}
	})
}

function waitForESEvent(es: EventSource, topic = 'test-topic') {
	return new Promise<MessageEvent>((resolve) => {
		es.addEventListener(topic, onMsg)
		function onMsg(event: MessageEvent) {
			resolve(event)
			es.removeEventListener('message', onMsg)
		}
	})
}
