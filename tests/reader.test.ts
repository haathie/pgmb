import assert from 'assert'
import { readFile } from 'fs/promises'
import { after, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import type { PoolClient } from 'pg'
import { Pool } from 'pg'

describe('PG Tests', () => {

	const pool = new Pool({ connectionString: process.env.PG_URI, max: 20 })
	let readerName: string

	before(async() => {
		await pool.query('DROP SCHEMA IF EXISTS pgmb2 CASCADE;')

		const sql = await readFile('./sql/pgmb2.sql', 'utf-8')
		await pool.query(sql)
	})

	after(async() => {
		await pool.end()
	})

	beforeEach(async() => {
		readerName = `reader_${Math.random().toString(36).substring(2, 15)}`
		await pool.query(
			'INSERT INTO pgmb2.readers (id) VALUES ($1)',
			[readerName]
		)
	})

	it('should receive events', async() => {
		const inserted = await insertEvent(pool)

		const rows = await readEvents(pool)
		assert.equal(rows.length, 1)
		assert.partialDeepStrictEqual(rows[0], inserted)

		const rows2 = await readEvents(pool)
		assert.equal(rows2.length, 0)
	})

	it('should not read uncommitted events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')
		const ins = await insertEvent(c1)

		assert.deepEqual(await readEvents(pool), [])

		await c1.query('COMMIT;')
		await c1.release()

		assert.partialDeepStrictEqual(await readEvents(pool), [ins])
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
			events.push(...await readEvents(pool, 25))
		}

		await writeEvents

		assert.equal(events.length, eventcount)

		await c1.release()

		assert.partialDeepStrictEqual(events, eventsWritten)
	})

	it('should not read future events', async() => {
		const c1 = await pool.connect()
		await c1.query('BEGIN;')
		const { rows: [prow, frow] } = await c1.query(
			`INSERT INTO pgmb2.events (id, topic, payload)
			VALUES
				(pgmb2.create_event_id(NOW()), 'test-topic', '{"a":1}'),
				(pgmb2.create_event_id(NOW() + interval '2 seconds'), 'test-topic', '{"a":2}')
			RETURNING *`,
		)

		assert.deepEqual(await readEvents(pool), [])

		await c1.query('COMMIT;')
		await c1.release()

		assert.partialDeepStrictEqual(await readEvents(pool), [prow])

		// check the tx was marked as completed
		const { rows: rState } = await pool.query(
			'select * from reader_xid_state where reader_id = $1',
			[readerName]
		)
		assert.ok(rState[0].completed_at)

		await setTimeout(2000)

		assert.partialDeepStrictEqual(await readEvents(pool), [frow])
	})

	it('should not read duplicate events', async() => {
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
			events.push(...await readEvents(pool, 30))
			// console.log(`Read ${events.length} / ${eventsToWrite} events so far`)
		}

		await task

		// ensure all events got read
		for(const ev of events) {
			assert.ok(
				eventsWritten
					.some(e => JSON.stringify(e.payload) === JSON.stringify(ev.payload))
			)
		}

		// ensure no duplicate events
		assert.equal(events.length, eventsToWrite)
	})


	it('should insert event and get subscriptions', async() => {
		await insertEvent(pool)
		const { rows: [sub] } = await pool.query<{ id: string }>(
			`INSERT INTO pgmb2.subscriptions (reader_id)
			VALUES ($1)
			RETURNING *`,
			[readerName]
		)

		const { rows } = await pool.query(
			'SELECT * FROM pgmb2.read_next_events_for_subscriptions($1)',
			[readerName]
		)
		assert.equal(rows.length, 1)
		assert.partialDeepStrictEqual(rows[0], { 'subscription_ids': [sub.id] })
	})

	it('should match subscriptions', async() => {
		await pool.query(
			`INSERT INTO pgmb2.events (topic, payload)
		VALUES ('test', '{"data": 0.7}'), ('test', '{"data": 0.3}')`,
		)
		const { rows: [, sub1, sub2] } = await pool.query<{ id: string }>(
			`INSERT INTO pgmb2.subscriptions (reader_id, conditions_sql, metadata)
			VALUES
				($1, 'e.payload->>''non_exist'' IS NOT NULL', DEFAULT),
				($1, $2, '{"min": 0.5}'),
				($1, $2, '{"min": 0}')
			RETURNING *`,
			[readerName, 'e.payload->\'data\' > s.metadata->\'min\'']
		)

		const { rows } = await pool.query(
			'SELECT * FROM pgmb2.read_next_events_for_subscriptions($1)',
			[readerName]
		)
		assert.equal(rows.length, 2)
		assert.partialDeepStrictEqual(
			rows,
			[
				// 0.7 > 0.5, and 0.7 > 0 -- so matched by both subs
				{ 'subscription_ids': [sub1.id, sub2.id] },
				// 0.3 !> 0.5, but 0.3 > 0 -- so matched only by sub2
				{ 'subscription_ids': [sub2.id] }
			]
		)
	})

	async function readEvents(client: Pool | PoolClient, count = 50) {
		const { rows } = await client.query<{ topic: string, payload: unknown }>(
			'SELECT * FROM pgmb2.read_next_events($1, $2)',
			[readerName, count]
		)
		return rows
	}
})

async function insertEvent(client: Pool | PoolClient) {
	const topic = 'test-topic'
	const payload = { data: Math.random() }
	await client.query(
		'INSERT INTO pgmb2.events (topic, payload) VALUES ($1, $2)',
		[topic, payload]
	)

	return { topic, payload }
}