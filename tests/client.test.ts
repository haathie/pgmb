import assert from 'assert'
import { readFile } from 'fs/promises'
import { after, afterEach, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { Pool, type PoolClient } from 'pg'
import { pino } from 'pino'
import { Pgmb2Client } from '../src/client2/index.ts'
import type { IReadNextEventsResult } from '../src/queries.ts'
import { reenqueueEventsForSubscription, writeEvents, writeScheduledEvents } from '../src/queries.ts'

const LOGGER = pino({ level: 'trace' })

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

		client = new Pgmb2Client({ client: pool, logger: LOGGER, poll: true, groupId })
		await client.init()
	})

	afterEach(async() => {
		await client.end()
	})

	it('should receive events', async() => {
		const inserted = await insertEvent(pool)

		const sub = await client.registerSubscription({}, true)
		const recv: unknown[] = []

		for await (const { items } of sub) {
			recv.push(...items)
			if(recv.length === 1) {
				await insertEvent(pool)
			}

			if(recv.length >= 2) {
				break
			}
		}

		assert.partialDeepStrictEqual(recv[0], inserted)

		const sub2 = await client.registerSubscription({}, true)
		const rslt = await Promise.race([sub2.next(), setTimeout(250)])
		assert.equal(rslt, undefined)
	})

	it('should not read uncommitted events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')
		const ins = await insertEvent(c1)

		const sub = await client.registerSubscription({}, true)
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
		await c1.release()

		await readPromise

		assert.partialDeepStrictEqual(received, [ins])
	})

	it('should receive events from concurrent transactions', async() => {
		const eventcount = 500
		const c1 = await pool.connect()
		await c1.query('BEGIN;')

		const events: unknown[] = []
		const sub = await client.registerSubscription({}, true)
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

		await c1.release()

		assert.partialDeepStrictEqual(events, eventsWritten)
	})

	it('should not read future events', async() => {
		const DELAY_MS = 1000

		const sub = await client.registerSubscription({}, true)

		const [prow, frow] = await writeScheduledEvents.run(
			{
				ts: [new Date(), new Date(Date.now() + DELAY_MS)],
				topics: ['test-topic', 'test-topic'],
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
		const sub = await client.registerSubscription({}, true)
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

			await c.release()
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

	it('should update poll fn when conditions_sql uniquely changed', async() => {
		const cond = 'e.topic = e.metadata->>\'topic\''
		const fnData0 = await getPollFnData()
		await client
			.registerSubscription({ groupId, conditionsSql: cond }, true)
		const fnData1 = await getPollFnData()
		assert.notEqual(fnData0.prosrc, fnData1.prosrc)

		const sub2 = await client
			.registerSubscription({ groupId, conditionsSql: cond }, true)
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

	it('should fail to create subscription with invalid conditions SQL', async() => {
		await assert.rejects(
			() => client.registerSubscription({ conditionsSql: 'INVALID SQL' }, true)
		)
	})

	it('should re-enqueue event for subscription', async() => {
		const sub1 = await client.registerSubscription({ groupId }, false)
		// control subscription
		const sub2 = await client.registerSubscription({ groupId }, false)

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
			{
				groupId,
				conditionsSql: "e.payload->>'non_exist' IS NOT NULL"
			},
			true
		)

		const noSubItems = Array.fromAsync(noSub)

		const sub1 = await client.registerSubscription(
			{
				groupId,
				conditionsSql: "e.payload->'data' > s.metadata->'min'",
				metadata: { min: 0.5 }
			},
			true
		)
		const sub2 = await client.registerSubscription(
			{
				groupId,
				conditionsSql: "e.payload->'data' > s.metadata->'min'",
				metadata: { min: 0 }
			},
			true
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
		assert.equal(sub1Nxt.value.items.length, 1)

		// 0.7 > 0.5, and 0.7 > 0 -- so matched by both subs
		const sub2Nxt = await sub2.next()
		assert(!sub2Nxt.done)
		assert.equal(sub2Nxt.value.items.length, 2)

		noSub.return?.()
		assert.deepEqual(await noSubItems, [])
	})

	it('should create events from table mutations', async() => {
		const sub = await client.registerSubscription(
			{ conditionsSql: "e.topic LIKE 'public.test_table.%%'" },
			false
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
					payload: { id: 1, data: 'hello!!!' },
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
})

async function insertEvent(client: Pool | PoolClient) {
	const topic = 'test-topic'
	const payload = { data: Math.random() }
	await writeEvents.run(
		{
			topics: [topic],
			payloads: [payload],
			metadatas: [{}]
		},
		client
	)

	return { topic, payload }
}