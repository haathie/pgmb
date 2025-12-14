import assert from 'assert'
import { Chance } from 'chance'
import type { ErrorEvent } from 'eventsource'
import { EventSource } from 'eventsource'
import { readFile } from 'fs/promises'
import type { Server, ServerResponse } from 'node:http'
import { createServer } from 'node:http'
import { after, afterEach, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { Pool, type PoolClient } from 'pg'
import { pino } from 'pino'
import { Pgmb2Client } from '../src/client2/index.ts'
import type { IReadNextEventsResult } from '../src/queries.ts'
import { pollForEvents, reenqueueEventsForSubscription, removeExpiredSubscriptions, writeEvents, writeScheduledEvents } from '../src/queries.ts'

const LOGGER = pino({ level: 'trace' })
const CHANCE = new Chance()

describe('PGMB Client Tests', () => {

	const pool = new Pool({ connectionString: process.env.PG_URI, max: 20 })

	let client: Pgmb2Client
	let groupId: string

	before(async() => {
		await pool.query('DROP SCHEMA IF EXISTS pgmb2 CASCADE;')

		const sql = await readFile('./sql/pgmb2.sql', 'utf-8')
		await pool.query(sql)
	})

	after(async() => {
		await pool.end()
	})

	beforeEach(async() => {
		groupId = `grp${Math.random().toString(36).substring(2, 15)}`

		client = new Pgmb2Client({
			client: pool,
			logger: LOGGER,
			poll: true,
			groupId,
			sleepDurationMs: 250,
			subscriptionMaintenanceMs: 1000
		})
		await client.init()
	})

	afterEach(async() => {
		await client.end()
	})

	it('should receive events on multiple subs', async() => {
		// we'll create 3 subs, 2 identical, 1 different
		// to check if de-duping works correctly on same conditions & params
		const sub1 = await client.registerSubscription({ })
		const sub2 = await client.registerSubscription({ })
		const sub3 = await client.registerSubscription({ params: { a: 1 } })

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
		assert.equal(sub2Recv.flatMap(s => s.items).length, 2)
		assert.equal(sub3Recv.flatMap(s => s.items).length, 2)

		// check old events aren't replayed, on a new subscription
		const sub4 = await client.registerSubscription({ params: { a: 4 } })
		const rslt = await Promise.race([sub4.next(), setTimeout(250)])
		assert.equal(rslt, undefined)
		sub4.return()
	})

	it('should remove expired subs', async() => {
		const sub = await client.registerSubscription({ expiryInterval: '1 second' })
		sub.return()

		await setTimeout(1500)

		const { rows: [{ count }] } = await pool.query<{ count: string }>(
			'select count(*) as count from pgmb2.subscriptions where id = $1',
			[sub.id]
		)
		assert.equal(count, '0')
	})

	it('should not read uncommitted events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')
		const ins = await insertEvent(c1)

		const sub = await client.registerSubscription({})
		const received: IReadNextEventsResult[] = []
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
		await client.registerSubscription({})
		await client.readChanges(groupId)
		await insertEvent(pool)
		const [changeCount1] = await Promise.all([
			client.readChanges(groupId),
			pollForEvents.run(undefined, pool),
		])
		const changeCount2 = await client.readChanges(groupId)
		assert.equal(changeCount1 + changeCount2, 1)
	})

	it('should receive events from concurrent transactions', async() => {
		const eventcount = 500
		const c1 = await pool.connect()
		await c1.query('BEGIN;')

		const events: unknown[] = []
		const sub = await client.registerSubscription({})
		const readPromise = (async() => {
			for await (const evs of sub) {
				events.push(...evs.items)
				if(events.length >= eventcount) {
					break
				}
			}
		})()

		const eventsWritten: unknown[] = []

		const writeEvents = (async() => {
			for(let i = 0; i < eventcount; i++) {
				eventsWritten.push(await insertEvent(c1))
			}

			await c1.query('COMMIT;')
		})()

		await Promise.all([readPromise, writeEvents])

		assert.equal(events.length, eventcount)

		c1.release()

		assert.partialDeepStrictEqual(events, eventsWritten)
	})

	it('should handle concurrent changes', async() => {
		await client.end()

		await Promise.all([
			Array.from({ length: 5 }).map(() => insertEvent(pool)),
			pollForEvents.run(undefined, pool),
			removeExpiredSubscriptions
				.run({ groupId: client.groupId, activeIds: [] }, pool),
			await pool.query(
				`select pgmb2.maintain_events_table(
					current_ts := NOW()
						+ pgmb2.get_config_value('partition_interval')::interval
				);`
			)
		])

		// check partitions exist
		const { rows: [{ count, expected }] } = await pool.query<{ count: string, expected: number }>(
			`SELECT
				count(*) as count,
				pgmb2.get_config_value('future_partitions_to_create')::int as expected
			FROM pg_catalog.pg_inherits
			WHERE inhparent = 'pgmb2.events'::regclass;`
		)
		// we'd removed 1 old partition by executing maintainence w a future
		// timestamp, which would've removed 1 old partition and created 1 new one
		// since by default we retain 2 oldest partitions, we should be 1 ahead
		assert.equal(+count, expected + 1)

		await client.init()
	})

	it('should not read future events', async() => {
		const DELAY_MS = 1000

		const topic = 'scheduled-event'
		const sub = await client.registerSubscription({
			conditionsSql: "s.params @> jsonb_build_object('topic', e.topic)",
			params: { topic: topic }
		})

		const [prow, frow] = await writeScheduledEvents.run(
			{
				ts: [new Date(), new Date(Date.now() + DELAY_MS)],
				topics: [topic, topic],
				payloads: [{ a: 1 }, { a: 2 }],
				metadatas: [{}, {}]
			},
			pool
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
		const sub = await client.registerSubscription({})
		const readPromise = (async() => {
			for await (const evs of sub) {
				events.push(...evs.items)
				if(events.length >= eventsToWrite) {
					break
				}
			}
		})()

		const eventsWritten: { payload: unknown }[] = []
		const task = Promise.all(Array.from({ length: writerCount }).map(async() => {
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
		}))

		await readPromise

		// ensure all events got read
		for(const ev of events) {
			assert.ok(
				eventsWritten
					.some(e => JSON.stringify(e.payload) === JSON.stringify(ev.payload))
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
		await pool.query('truncate pgmb2.subscriptions;')

		const EVENT_COUNT = 10_000
		const SUB_PER_TYPE_COUNT = 1_000
		const SUB_TYPES = 4
		const TOTAL_SUB_COUNT = SUB_PER_TYPE_COUNT * SUB_TYPES

		for(let i = 0; i < SUB_TYPES;i++) {
			const k = `key${i}`
			await pool.query<{ id: string }>(
				`insert into pgmb2.subscriptions (group_id, conditions_sql, params)
				select * from unnest($1::text[], $2::text[], $3::jsonb[])
				 as t(group_id, conditions_sql, params)
				returning id`,
				[
					Array.from({ length: SUB_PER_TYPE_COUNT }).map(() => groupId),
					Array.from({ length: SUB_PER_TYPE_COUNT }).map(() => (
						`s.params @> jsonb_build_object(\'${k}\', e.payload->>\'key\')`
					)),
					Array.from({ length: SUB_PER_TYPE_COUNT }).map((_, i) => ({ [k]: i.toString() }))
				]
			)
		}

		await writeEvents.run(
			{
				topics: Array.from({ length: EVENT_COUNT })
					.map(() => 'test-topic-1'),
				payloads: Array.from({ length: EVENT_COUNT })
					.map((_, i) => ({	key: (i % SUB_PER_TYPE_COUNT).toString() })),
				metadatas: Array.from({ length: EVENT_COUNT }).map(() => ({}))
			},
			pool
		)

		console.log('Inserted events and subs, starting poll...')

		const now = Date.now()
		const [{ count }] = await pollForEvents.run(undefined, pool)

		const tt = Date.now() - now
		console.log(`Processed ${count} events for ${TOTAL_SUB_COUNT} subs in ${tt}ms`)

		// ensure it took less than 1 second
		assert(tt <= 1000, `Took too long: ${tt}ms`)

		assert.equal(count, SUB_TYPES * EVENT_COUNT)

		await pool.query('truncate pgmb2.subscription_events;')

		await client.init()
	})

	it('should update poll fn when conditions_sql uniquely changed', async() => {
		const cond = 'e.topic = s.params->>\'topic\''
		const fnData0 = await getPollFnData()
		await client
			.registerSubscription({ conditionsSql: cond, params: { topic: 'test1' } })
		const fnData1 = await getPollFnData()
		assert.notEqual(fnData0.prosrc, fnData1.prosrc)

		const sub2 = await client
			.registerSubscription({ conditionsSql: cond, params: { topic: 'test2' } })
		const fnData2 = await getPollFnData()
		assert.deepEqual(fnData1.prosrc, fnData2.prosrc)
		await sub2.return?.()

		await setTimeout(500)

		const fnData3 = await getPollFnData()
		assert.deepEqual(fnData2.prosrc, fnData3.prosrc)

		async function getPollFnData() {
			const { rows: [row] } = await pool.query(
				'select * from pg_proc where proname = \'poll_for_events\''
			)
			assert(row)
			return row
		}
	})

	it('should concurrently update poll fn', async() => {
		const conds = [
			'e.topic = s.params->>\'topic\'',
			"e.payload->>'data' = s.params->>'data'",
			"(e.payload->>'value')::int > (s.params->>'min_value')::int"
		]
		await Promise.all([
			client.readChanges(groupId),
			...conds.map(cond => (
				client.registerSubscription({ conditionsSql: cond })
			))
		])

		await client.readChanges(groupId)

		const { rows: [procRow] } = await pool.query<{ prosrc: string }>(
			'select prosrc from pg_proc where proname = \'poll_for_events\''
		)
		for(const cond of conds) {
			assert.ok(procRow.prosrc.includes(cond))
		}
	})

	it('should fail to create subscription with invalid conditions SQL', async() => {
		await assert.rejects(
			() => client.registerSubscription({ conditionsSql: 'INVALID SQL' })
		)
	})

	it('should re-enqueue event for subscription', async() => {
		const sub1 = await client.registerSubscription({ conditionsSql: 'TRUE' })
		// control subscription
		const sub2 = await client.registerSubscription({ conditionsSql: 'TRUE AND TRUE' })

		await insertEvent(pool)

		const [{ done, value }] = await Promise.all([sub1.next(), sub2.next()])
		assert(!done)
		assert.equal(value.items.length, 1)

		const now = Date.now()
		const reSubId = value.items[0].subscriptionIds[0]
		await reenqueueEventsForSubscription.run(
			{
				eventIds: [value.items[0].id],
				subscriptionId: reSubId,
				offsetInterval: '1 second'
			},
			pool
		)

		const e2 = await Promise.race([sub1.next(), sub2.next()])
		assert(!e2.done)

		// shouldve come after at least 1 second
		assert.ok(Date.now() - now >= 1000)

		assert.equal(e2.value.items.length, 1)
		assert.deepEqual(e2.value.items[0].subscriptionIds, [reSubId])
	})

	it('should match subscriptions', async() => {
		const noSub = await client.registerSubscription(
			{ conditionsSql: "e.payload->>'non_exist' IS NOT NULL" }
		)

		const noSubItems = Array.fromAsync(noSub)

		const sub1 = await client.registerSubscription(
			{
				conditionsSql: "e.payload->'data' > s.params->'min'",
				params: { min: 0.5 }
			}
		)
		const sub2 = await client.registerSubscription(
			{
				conditionsSql: "e.payload->'data' > s.params->'min'",
				params: { min: 0 }
			}
		)

		await writeEvents.run(
			{
				topics: ['test', 'test'],
				payloads: [{ data: 0.7 }, { data: 0.3 }],
				metadatas: [{}, {}]
			},
			pool
		)

		// 0.3 !> 0.5, but 0.3 > 0 -- so matched only by sub2
		const sub1Nxt = await sub1.next()
		assert(!sub1Nxt.done)
		assert.deepEqual(sub1Nxt.value.metadata, { min: 0.5 })
		assert.equal(sub1Nxt.value.items.length, 1)

		// 0.7 > 0.5, and 0.7 > 0 -- so matched by both subs
		const sub2Nxt = await sub2.next()
		assert(!sub2Nxt.done)
		assert.deepEqual(sub2Nxt.value.metadata, { min: 0 })
		assert.equal(sub2Nxt.value.items.length, 2)

		noSub.return?.()
		assert.deepEqual(await noSubItems, [])
	})

	it('should create events from table mutations', async() => {
		const sub = await client.registerSubscription(
			{
				params: {
					topics: [
						'public.test_table.insert',
						'public.test_table.update',
						'public.test_table.delete'
					]
				},
				conditionsSql: 's.params @> (\'{"topics":["\' || e.topic || \'"]}\')::jsonb'
			}
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
			SELECT pgmb2.push_table_mutations('public.test_table'::regclass);
		`)

		await pool.query(`
			INSERT INTO public.test_table (data) VALUES ('hello'), ('world');
		`)
		await pool.query(
			'UPDATE public.test_table SET data = \'hello!!!\' WHERE id = 1;'
		)
		await pool.query(
			'DELETE FROM public.test_table WHERE id = 2;'
		)

		while(!events.length) {
			await setTimeout(50)
		}

		assert.equal(events.length, 4)
		assert.partialDeepStrictEqual(
			events,
			[
				{ topic: 'public.test_table.insert', payload: { id: 1, data: 'hello' } },
				{ payload: { id: 2, data: 'world' } },
				{
					topic: 'public.test_table.update',
					payload: { data: 'hello!!!' },
					metadata: { old: { id: 1, data: 'hello' } }
				},
				{ topic: 'public.test_table.delete', payload: { id: 2 } }
			]
		)

		// check removing subscribable works
		await pool.query(`
			SELECT pgmb2.stop_table_mutations_push('public.test_table'::regclass);
			INSERT INTO public.test_table (data) VALUES ('new data');
		`)

		// ensure no new events come in
		await setTimeout(500)
		sub.return?.()

		await task
		assert.equal(events.length, 4)
	})

	describe('SSE', () => {

		let srv: Server
		let latestSrvRes: ServerResponse
		const port: number = CHANCE.integer({ min: 10000, max: 65000 })

		before(async() => {
			srv = createServer()
			await new Promise<void>(resolve => (
				srv.listen(port, resolve)
			))
			srv.on('request', async(req, res) => {
				latestSrvRes = res
				await client.registerSseSubscription({}, req, res)
			})
		})

		after(async() => {
			srv.close()
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
			assert.deepEqual(client.subscribers, {})
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
			await client.removeSubscription(Object.keys(client.subscribers)[0])
			// removal error
			await assert.rejects(waitForESOpen(es))
			// reconnection error
			await assert.rejects(
				waitForESOpen(es),
				(err: ErrorEvent | undefined) => {
					assert.equal(err?.code, 204)
					return true
				}
			)
		})

		async function openEs() {
			const es = new EventSource(`http://localhost:${port}/sse`)
			await waitForESOpen(es)

			assert(latestSrvRes)
			return { es, res: latestSrvRes }
		}
	})
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

async function insertEvent(client: Pool | PoolClient) {
	const topic = 'test-topic'
	const payload = { data: Math.random() }
	const [{ id }] = await writeEvents.run(
		{
			topics: [topic],
			payloads: [payload],
			metadatas: [{}]
		},
		client
	)

	return { id, topic, payload }
}
