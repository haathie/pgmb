import assert from 'assert'
import { readFile } from 'fs/promises'
import { after, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import type { PoolClient } from 'pg'
import { Pool } from 'pg'

describe('Reader Tests', () => {

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

	it.only('should not read duplicate events', async() => {
		const writerCount = 5
		const eventsPerWriter = 200
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

					await setTimeout(Math.floor(Math.random() * 50))
				}
			}

			if(state === 'in-tx') {
				await c.query('COMMIT;')
			}

			await c.release()
		}))
			.then(() => console.log('Writers completed'))

		const events: { payload: unknown }[] = []
		while(events.length < eventsToWrite) {
			events.push(...await readEvents(pool, 25))
			console.log(`Read ${events.length} / ${eventsToWrite} events so far`)
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