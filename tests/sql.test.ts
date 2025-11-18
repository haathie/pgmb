import assert from 'assert'
import { readFile } from 'fs/promises'
import { after, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import type { PoolClient } from 'pg'
import { Pool } from 'pg'
import { createSubscription, pollForEvents, readNextEvents, readNextEventsForGroup, reenqueueEventsForSubscription, writeEvents, writeScheduledEvents } from '../src/queries.ts'

describe('SQL Tests', () => {

	const pool = new Pool({ connectionString: process.env.PG_URI, max: 20 })
	let mainSubName: string

	before(async() => {
		await pool.query('DROP SCHEMA IF EXISTS pgmb2 CASCADE;')

		const sql = await readFile('./sql/pgmb2.sql', 'utf-8')
		await pool.query(sql)
	})

	after(async() => {
		await pool.end()
	})

	beforeEach(async() => {
		mainSubName = `sb${Math.random().toString(36).substring(2, 15)}`
		await createSubscription.run({ id: mainSubName }, pool)
	})

	it('should receive events', async() => {
		const inserted = await insertEvent(pool)

		const rows = await pollAndReadEvents(pool)
		assert.equal(rows.length, 1)
		assert.partialDeepStrictEqual(rows[0], inserted)

		const rows2 = await pollAndReadEvents(pool)
		assert.equal(rows2.length, 0)
	})

	it('should not read uncommitted events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')
		const ins = await insertEvent(c1)

		assert.deepEqual(await pollAndReadEvents(pool), [])

		await c1.query('COMMIT;')
		await c1.release()

		assert.partialDeepStrictEqual(await pollAndReadEvents(pool), [ins])
	})

	it('should receive events from concurrent transactions', async() => {
		const eventcount = 500
		const c1 = await pool.connect()
		await c1.query('BEGIN;')

		const eventsWritten: unknown[] = []

		const writeEvents = (async() => {
			for(let i = 0; i < eventcount; i++) {
				eventsWritten.push(await insertEvent(c1))
			}

			await c1.query('COMMIT;')
		})()

		const events: unknown[] = []
		while(events.length < eventcount) {
			events.push(...await pollAndReadEvents(pool, 25))
		}

		await writeEvents

		assert.equal(events.length, eventcount)

		await c1.release()

		assert.partialDeepStrictEqual(events, eventsWritten)
	})

	it('should not read future events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')

		const DELAY_MS = 1000

		const [prow, frow] = await writeScheduledEvents.run(
			{
				ts: [new Date(), new Date(Date.now() + DELAY_MS)],
				topics: ['test-topic', 'test-topic'],
				payloads: [{ a: 1 }, { a: 2 }],
				metadatas: [{}, {}]
			},
			c1
		)

		assert.deepEqual(await pollAndReadEvents(pool), [])

		await c1.query('COMMIT;')
		await c1.release()

		assert.partialDeepStrictEqual(await pollAndReadEvents(pool), [prow])

		await setTimeout(DELAY_MS)

		assert.partialDeepStrictEqual(await pollAndReadEvents(pool), [frow])
	})

	it.only('should not read duplicate events', async() => {
		const writerCount = 10
		const eventsPerWriter = 300
		const eventsToWrite = writerCount * eventsPerWriter

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
		// .then(() => console.log('Writers completed'))

		const events: { payload: unknown }[] = []
		while(events.length < eventsToWrite) {
			events.push(...await pollAndReadEvents(pool, 30))
		}

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

	it('should re-enqueue event for subscription', async() => {
		const groupId = 'reader1'
		const [sub1] = await createSubscription.run({ groupId }, pool)

		// control subscription
		await createSubscription.run({ groupId }, pool)

		await insertEvent(pool)

		const rows = await pollAndReadGroup(pool, groupId)
		assert.equal(rows.length, 1)

		await reenqueueEventsForSubscription.run(
			{
				eventIds: rows.map(r => r.id),
				subscriptionId: sub1.id,
				offsetInterval: '1 second'
			},
			pool
		)

		assert.deepEqual(await pollAndReadGroup(pool, groupId), [])

		await setTimeout(1000)

		assert.partialDeepStrictEqual(
			await pollAndReadGroup(pool, groupId),
			[{ subscriptionIds: [sub1.id] }]
		)
	})

	it('should match subscriptions', async() => {
		const groupId = 'group2'

		await createSubscription.run(
			{
				groupId,
				conditionsSql: "e.payload->>'non_exist' IS NOT NULL"
			},
			pool
		)
		const [sub1] = await createSubscription.run(
			{
				groupId,
				conditionsSql: "e.payload->'data' > s.metadata->'min'",
				metadata: { min: 0.5 }
			},
			pool
		)
		const [sub2] = await createSubscription.run(
			{
				groupId,
				conditionsSql: "e.payload->'data' > s.metadata->'min'",
				metadata: { min: 0 }
			},
			pool
		)

		await writeEvents.run(
			{
				topics: ['test', 'test'],
				payloads: [{ data: 0.7 }, { data: 0.3 }],
				metadatas: [{}, {}]
			},
			pool
		)

		const rows = await pollAndReadGroup(pool, groupId)
		assert.equal(rows.length, 2)
		// 0.7 > 0.5, and 0.7 > 0 -- so matched by both subs
		assert.deepStrictEqual(
			rows[0].subscriptionIds, [sub1.id, sub2.id]
		)
		// 0.3 !> 0.5, but 0.3 > 0 -- so matched only by sub2
		assert.deepStrictEqual(
			rows[1].subscriptionIds, [sub2.id]
		)
	})

	it('should create events from table mutations', async() => {
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

		const rows = await pollAndReadEvents(pool, 10)
		assert.equal(rows.length, 4)
		assert.partialDeepStrictEqual(
			rows,
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

		const moreRows = await pollAndReadEvents(pool, 10)
		assert.equal(moreRows.length, 0)
	})

	async function pollAndReadGroup(client: Pool | PoolClient, groupId: string) {
		await pollForEvents.run(undefined, client)
		const rows = await readNextEventsForGroup
			.run({ groupId, chunkSize: 10 }, client)
		return rows
	}

	async function pollAndReadEvents(client: Pool | PoolClient, count = 50) {
		await pollForEvents.run(undefined, client)
		const rows = await readNextEvents
			.run({ subscriptionId: mainSubName, chunkSize: count }, client)
		return rows
	}
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