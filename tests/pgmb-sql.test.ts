import assert from 'assert'
import { Chance } from 'chance'
import { randomBytes } from 'crypto'
import { Pool, type PoolClient } from 'pg'
import type { PgEnqueueMsg, PgPublishMsg } from '../src/types.ts'
import { delay, getDateFromMessageId, serialisePgMsgConstructorsIntoSql } from '../src/utils.ts'
import { getQueueSchemaName, isQueueLogged, send } from './utils.ts'

const chance = new Chance()

// unit tests
describe('PGMB SQL Tests', () => {

	const pool = new Pool({ connectionString: process.env.PG_URI, max: 20 })

	afterAll(async() => {
		await pool.end()
	})

	it('should create unique message IDs', async() => {
		const genCount = 5000
		const parallelCount = 20
		const totalSet = new Set<string>()
		const totalRows = await Promise.all(
			Array.from({ length: parallelCount }, async() => {
				const { rows } = await pool.query(
					`WITH rand_data AS (
						SELECT pgmb.create_random_bigint() AS rand
					)
					SELECT
						pgmb.create_message_id(rand := rand + additive) AS id
					from generate_series(1, ${genCount}) AS t(additive)
					LEFT JOIN rand_data ON TRUE`
				)
				return rows
			})
		)
		for(const rows of totalRows) {
			expect(rows.length).toBe(genCount)
			const sorted = [...rows].sort((a, b) => a.id - b.id)
			expect(sorted).toEqual(rows)

			for(const { id } of rows) {
				expect(totalSet).not.toContain(id)
				totalSet.add(id)
			}
		}

		expect(totalSet.size).toBe(genCount * parallelCount)
	}, 30000)

	it('should create sequential unique message IDs with a date', async() => {
		const genCount = 100
		const { rows } = await pool.query(
			`select
				pgmb.create_message_id(
					dt => $1::timestamptz,
					rand => num
				) AS id
			from generate_series(1, ${genCount}) AS t(num) ORDER BY num`,
			[new Date().toJSON()]
		)
		expect(rows.length).toBe(genCount)
		const sorted = [...rows].sort((a, b) => a.id.localeCompare(b.id))
		expect(sorted).toEqual(rows)
		const rowSet = new Set<string>(rows.map(r => r.id))
		expect(rowSet.size).toBe(genCount)
	})

	it('should correctly get the message ID date', async() => {
		const dt = new Date('2023-10-01T00:00:00Z')
		const { rows: [{ id }] } = await pool.query(
			'SELECT pgmb.create_message_id($1::timestamptz) AS id', [dt.toJSON()]
		)
		assert(typeof id === 'string')
		const { rows: [{ date }] } = await pool.query(
			'SELECT pgmb.extract_date_from_message_id($1) AS date',
			[id]
		)
		expect(date).toEqual(dt)

		const dateJs = getDateFromMessageId(id)
		expect(dateJs).toEqual(dt)
	})

	testWithRollback('should create a logged queue', async client => {
		const queueName = createQueueName()
		const { rows: [{ created }] } = await client.query(
			'SELECT pgmb.assert_queue($1) as "created"', [queueName]
		)
		// second time should be a no-op
		const { rows: [{ created: created2 }] } = await client.query(
			'SELECT pgmb.assert_queue($1) as "created"', [queueName]
		)
		expect(created).toBeTruthy()
		expect(created2).toBeFalsy()

		// check if the table is a logged table
		expect(await isQueueLogged(client, queueName)).toBe(true)

		const { rows } = await client.query(
			'SELECT * FROM pgmb.queues WHERE name = $1', [queueName]
		)
		expect(rows.length).toBe(1)
		expect(rows[0].name).toBe(queueName)
		expect(rows[0]['schema_name']).toBeTruthy()

		// ensure schema tables exist
		const { rows: tables } = await client.query(
			`SELECT table_name FROM information_schema.tables
			WHERE table_schema = $1`, [rows[0]['schema_name']]
		)
		expect(tables.length).toBeGreaterThan(0)
	})

	testWithRollback('should create a queue w archive table', async client => {
		const queueName = createQueueName()
		const { rows: [{ created }] } = await client.query(
			'SELECT pgmb.assert_queue($1, \'archive\') as "created"', [queueName]
		)
		expect(created).toBeTruthy()

		const { rows } = await client.query(
			'SELECT * FROM pgmb.queues WHERE name = $1', [queueName]
		)
		expect(rows.length).toBe(1)
		expect(rows[0].name).toBe(queueName)
		expect(rows[0]['ack_setting']).toBe('archive')

		// ensure archived table exists
		await client.query(
			`SELECT * FROM ${getQueueSchemaName(queueName)}.consumed_messages`
		)
	})

	testWithRollback('should create an unlogged queue', async client => {
		const queueName = createQueueName()
		const { rows: [{ created }] } = await client.query(
			'SELECT pgmb.assert_queue($1, queue_type => $2) as "created"',
			[queueName, 'unlogged']
		)
		expect(created).toBeTruthy()

		// check if the table is a logged table
		expect(await isQueueLogged(client, queueName)).toBe(false)

		const { rows } = await client.query(
			'SELECT * FROM pgmb.queues WHERE name = $1', [queueName]
		)
		expect(rows.length).toBe(1)
		expect(rows[0].name).toBe(queueName)
		expect(rows[0]['schema_name']).toBeTruthy()
		expect(rows[0]['queue_type']).toBe('unlogged')
	})

	testWithRollback('should send message to a queue', async client => {
		const queueName = createQueueName()
		await client.query(
			// add default headers to the queue
			'SELECT pgmb.assert_queue($1, \'archive\', \'{"test":"1"}\'::jsonb)',
			[queueName]
		)

		const msg: PgEnqueueMsg = {
			message: randomBytes(256),
			headers: { foo: 'bar' },
			consumeAt: new Date(Date.now() - 500),
		}
		const msgsCreated = await send(client, queueName, [msg])
		expect(msgsCreated.length).toBe(1)
		expect(msgsCreated[0].id).toBeTruthy()

		// ensure message is scheduled at the correct time
		const { rows: [{ dt }] } = await client.query(
			'SELECT pgmb.extract_date_from_message_id($1) AS dt', [msgsCreated[0].id]
		)
		expect(dt).toEqual(msg.consumeAt)

		const { rows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName]
		)
		expect(rows.length).toEqual(1)
		expect(rows[0].message).toEqual(msg.message)
		// ensure default headers & the message's headers are merged
		expect(rows[0].headers).toEqual({ foo: 'bar', test: '1' })
	})

	testWithRollback('should delete a queue', async client => {
		const queueName = createQueueName()
		await client.query('SELECT pgmb.assert_queue($1)', [queueName])
		await client.query('SELECT pgmb.delete_queue($1)', [queueName])

		const { rows } = await client.query(
			'SELECT * FROM pgmb.queues WHERE name = $1', [queueName]
		)
		expect(rows.length).toBe(0)

		const { rowCount } = await client.query(
			`SELECT * FROM information_schema.tables
			WHERE table_schema = $1`, [getQueueSchemaName(queueName)]
		)
		expect(rowCount).toBe(0)
	})

	it('should block a message when a consumer has read it', async() => {
		const queueName = createQueueName()
		const rowsToRead = 3

		const client = await pool.connect()
		await client.query('SELECT pgmb.assert_queue($1)', [queueName])
		const msgs = Array.from({ length: 10 }, (): PgEnqueueMsg => ({
			message: randomBytes(256),
		}))
		const msgsCreated = await send(client, queueName, msgs)
		expect(msgsCreated.length).toBe(msgs.length)

		await client.query('BEGIN')
		const { rows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, $2)', [queueName, rowsToRead]
		)

		const client2 = await pool.connect()
		const { rows: rows2 } = await client2.query(
			'SELECT * FROM pgmb.read_from_queue($1, $2)', [queueName, rowsToRead]
		)

		const rowIdSet = new Set([
			...rows.map(r => r.id),
			...rows2.map(r => r.id)
		])
		expect(rowIdSet.size).toBe(rowsToRead * 2)

		await client.query('SELECT pgmb.delete_queue($1)', [queueName])
		await client.query('COMMIT')
		client.release()
		client2.release()
	})

	testWithRollback('should purge a queue', async client => {
		const queueName = createQueueName()
		await client.query('SELECT pgmb.assert_queue($1)', [queueName])
		await send(client, queueName, [ { message: 'data_1' } ])

		await client.query('SELECT pgmb.purge_queue($1)', [queueName])
		const { rows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName]
		)
		expect(rows.length).toBe(0)
	})

	testWithRollback('should correctly order multiple messages to a queue', async client => {
		const queueName = createQueueName()
		await client.query('SELECT pgmb.assert_queue($1)', [queueName])
		const msgs = Array.from({ length: 100 }, (_, i): PgEnqueueMsg => ({
			message: 'data_' + i.toString(16).padStart(3, '0')
		}))
		const msgsCreated = await send(client, queueName, msgs)
		expect(msgsCreated.length).toBe(msgs.length)

		const { rows } = await client
			.query('SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName])
		for(const row of rows) {
			row.message = row.message.toString()
		}

		const rowsSorted = [...rows].sort((a, b) => (
			a.message.localeCompare(b.message)
		))
		expect(rows).toEqual(rowsSorted)
	})

	testWithRollback('should correctly ack a msg', async client => {
		const queueName = createQueueName()
		await client.query('SELECT pgmb.assert_queue($1, \'archive\')', [queueName])

		const rows = await send(client, queueName, [
			{ message: 'data_1' },
			{ message: 'data_2' },
		])
		await client.query(
			'SELECT pgmb.ack_msgs($1, true, $2::varchar[])',
			[queueName, `{${rows[0].id}}`]
		)
		const { rows: ackedRows } = await client.query(
			`SELECT * FROM ${getQueueSchemaName(queueName)}.consumed_messages`
		)
		expect(ackedRows.length).toBe(1)
		expect(ackedRows[0].id).toBe(rows[0].id)
		expect(ackedRows[0].success).toBe(true)

		// ensure the other message is still in the live queue
		const { rows: liveRows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName]
		)
		expect(liveRows.length).toBe(1)
		expect(liveRows[0].id).toBe(rows[1].id)
	})

	testWithRollback('should correctly retry a msg', async client => {
		const queueName = createQueueName()
		const retriesLeftS = [1, 2]
		await client.query(
			'SELECT pgmb.assert_queue($1, \'archive\', $2::jsonb)',
			// default headers to retry the message twice
			// once after 1 second, and once after 2 seconds
			[queueName, JSON.stringify({ retriesLeftS })]
		)

		const msgsToSend: PgEnqueueMsg[] = [
			{ message: 'data_1' },
			{ message: 'data_2' },
		]

		const msgsCreated = await send(client, queueName, msgsToSend)
		const nackId = msgsCreated.at(-1)?.id
		const nackMsg = msgsToSend.at(-1)?.message
		await client.query(
			'SELECT pgmb.ack_msgs($1, false, $2::varchar[])',
			[queueName, `{${nackId}}`]
		)

		for(const retryS of retriesLeftS) {
			// ensure the message is not readable right now
			const { rows: curRows } = await client.query(
				'SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName]
			)
			expect(curRows.length).toBe(1)

			// message should be readable after the retry time
			await delay(retryS * 1000 + 200)

			const { rows } = await client.query(
				'SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName]
			)
			expect(rows.length).toBe(2)

			const nackRow = rows.at(-1)
			expect(nackRow.headers).toMatchObject({
				originalMessageId: nackId,
				tries: expect.any(Number),
			})
			expect(nackRow.message)
				.toEqual(Buffer.from(nackMsg as string))
			await client.query(
				'SELECT pgmb.ack_msgs($1, false, $2::varchar[])',
				[queueName, `{${nackRow.id}}`]
			)
		}

		// ensure the message is not in the table anymore
		const { rows: liveRows } = await client.query(
			`SELECt * FROM ${getQueueSchemaName(queueName)}.live_messages`
		)
		expect(liveRows.length).toBe(1)

		// check the message is in the consumed table
		const { rows: ackedRows } = await client.query(
			`SELECT * FROM ${getQueueSchemaName(queueName)}.consumed_messages`
		)
		// 1 initial try + 2 retries
		expect(ackedRows.length).toBe(3)
	})

	testWithRollback('should get metrics for a queue', async client => {
		const queueName = createQueueName()
		await client.query('SELECT pgmb.assert_queue($1)', [queueName])
		await send(client, queueName, [
			{ message: 'data_1' },
			{ message: 'data_2' },
			{ message: 'data_3', consumeAt: new Date(Date.now() + 5500) },
		])

		const { rows } = await client.query(
			'SELECT total_length, consumable_length'
				+ ', EXTRACT(epoch FROM newest_msg_age) AS newest_msg_age'
				+ ', EXTRACT(epoch FROM oldest_msg_age) AS oldest_msg_age'
				+ ' FROM pgmb.get_queue_metrics($1, false)',
			[queueName]
		)
		expect(rows.length).toBe(1)
		const [{
			total_length: total,
			consumable_length: consumable,
			newest_msg_age: newestMsgAge,
			oldest_msg_age: oldestMsgAge,
		}] = rows
		expect(total).toBe(3)
		expect(consumable).toBe(2)
		expect(+newestMsgAge).toBeLessThanOrEqual(-5)
		expect(+oldestMsgAge).toBeLessThan(0.5)
	})

	testWithRollback('should get approx metrics for a queue', async client => {
		const queueName = createQueueName()
		await client.query('SELECT pgmb.assert_queue($1)', [queueName])
		await send(client, queueName, [
			{ message: 'data_1' },
			{ message: 'data_2' },
			{ message: 'data_3', consumeAt: new Date(Date.now() + 5500) },
		])

		const { rows } = await client.query(
			'SELECT total_length, consumable_length'
				+ ', EXTRACT(epoch FROM newest_msg_age) AS newest_msg_age'
				+ ', EXTRACT(epoch FROM oldest_msg_age) AS oldest_msg_age'
				+ ' FROM pgmb.get_queue_metrics($1, approximate => true)',
			[queueName]
		)
		expect(rows.length).toBe(1)
		const [{
			total_length: total,
			consumable_length: consumable,
			newest_msg_age: newestMsgAge,
			oldest_msg_age: oldestMsgAge,
		}] = rows
		expect(typeof total).toBe('number')
		expect(typeof consumable).toBe('number')
		expect(+newestMsgAge).toBeLessThanOrEqual(-5)
		expect(+oldestMsgAge).toBeLessThan(0.5)
	})

	testWithRollback('should create an exchange & bind queue', async client => {
		const queueNames = Array.from({ length: 2 }, createQueueName)
		const exchangeName = createExchangeName()

		await client.query('SELECT pgmb.assert_exchange($1)', [exchangeName])
		for(const queueName of queueNames) {
			await client.query('SELECT pgmb.assert_queue($1)', [queueName])
			await client.query(
				'SELECT pgmb.bind_queue($1, $2)', [queueName, exchangeName]
			)
			// bind again to ensure it is not added again
			await client.query(
				'SELECT pgmb.bind_queue($1, $2)', [queueName, exchangeName]
			)
		}

		const { rows } = await client.query(
			'SELECT * FROM pgmb.exchanges WHERE name = $1', [exchangeName]
		)

		expect(rows.length).toBe(1)
		expect(rows[0].name).toBe(exchangeName)
		expect(rows[0]['queues']).toEqual(queueNames)
	})

	testWithRollback('should publish to an exchange', async client => {
		const queues = Array.from({ length: 2 }, createQueueName)
		// nothing should be published to this queue
		const controlQueue = createQueueName()
		const exchangeName = createExchangeName()

		await client.query('SELECT pgmb.assert_exchange($1)', [exchangeName])

		for(const queueName of queues) {
			await client.query(
				'SELECT pgmb.assert_queue($1, \'archive\', $2::jsonb)',
				// add some default headers to the queue -- to ensure, that
				// after the fanout, the message will have the default headers
				// of the respective queue
				[queueName, JSON.stringify({ queueName })]
			)
			await client.query(
				'SELECT pgmb.bind_queue($1, $2)', [queueName, exchangeName]
			)
		}

		await client.query('SELECT pgmb.assert_queue($1)', [controlQueue])

		const msgs: PgPublishMsg[] = Array.from(
			{ length: 50 }, (_, i): PgPublishMsg => ({
				exchange: exchangeName,
				message: randomBytes(256),
				headers: { foo: 'bar', msgId: i },
				consumeAt: new Date(Date.now() - 500),
			})
		)
		const [sql, params] = serialisePgMsgConstructorsIntoSql(msgs, [])
		const { rows } = await client
			.query(`SELECT pgmb.publish(${sql}) AS id`, params)
		expect(rows).toHaveLength(msgs.length)

		for(const queueName of queues) {
			const { rows: queueRows } = await client.query(
				'SELECT * FROM pgmb.read_from_queue($1, 100)', [queueName]
			)
			expect(queueRows.length).toBe(msgs.length)
			for(const [i, msg] of queueRows.entries()) {
				expect(queueRows[i].message).toEqual(msg.message)
				expect(queueRows[i].headers).toMatchObject({
					...msg.headers,
					// queue name added from the default headers configured
					// in the queue
					queueName,
					// the exchange fanout will add the exchange name
					exchange: exchangeName,
				})
				expect(queueRows[i].id).toBe(rows[i].id)
			}
		}

		// ensure the control queue is empty
		const { rows: controlRows } = await client.query(
			'SELECT * FROM pgmb.read_from_queue($1, 100)', [controlQueue]
		)
		expect(controlRows.length).toBe(0)
	})

	testWithRollback('should publish to multiple exchanges with various number of bindings', async client => {
		const queues = Array.from({ length: 3 }, createQueueName)
		const msgsToSend = 2500
		const pairs = [
			{ exchange: createExchangeName(), queues: [queues[0]] },
			{ exchange: createExchangeName(), queues: [] },
			{
				exchange: createExchangeName(),
				queues: [queues[1], queues[2]]
			},
			{
				exchange: createExchangeName(),
				queues: queues
			},
		]

		for(const { exchange, queues } of pairs) {
			await client.query('SELECT pgmb.assert_exchange($1)', [exchange])
			for(const queue of queues) {
				await client.query('SELECT pgmb.assert_queue($1)', [queue])
				// ensure the queue is bound to the exchange
				await client.query(
					'SELECT pgmb.bind_queue($1, $2)', [queue, exchange]
				)
			}
		}

		const msgs = Array.from({ length: msgsToSend }, (_, i): PgPublishMsg => ({
			exchange: chance.pickone(pairs).exchange,
			message: randomBytes(16),
			headers: { customkey: i },
		}))
		const [sql, params] = serialisePgMsgConstructorsIntoSql(msgs, [])
		const { rows } = await client
			.query(`SELECT pgmb.publish(${sql}) AS id`, params)
		expect(rows.length).toBe(msgs.length)

		// map of queue to which msgs should've been sent to it
		const queueMsgMap: Record<string, { id: string, msg: Uint8Array | string }[]> = {}
		// ensure that we got the messages in the right order,
		// and NULL was sent back for exchanges that had no queues bound
		for(const [i, msg] of msgs.entries()) {
			const exchangeData = pairs.find(p => p.exchange === msg.exchange)
			if(!exchangeData?.queues.length) {
				// if there are no queues bound to the exchange, the message
				// should not be queued
				expect(rows[i].id).toBeNull()
				continue
			}

			expect(rows[i].id).toBeTruthy()
			for(const queue of exchangeData.queues) {
				queueMsgMap[queue] ||= []
				queueMsgMap[queue].push({ id: rows[i].id, msg: msg.message })
			}
		}

		for(const queue of queues) {
			const msgIds = queueMsgMap[queue] || []
			expect(msgIds.length).toBeGreaterThan(0)

			const { rows: queueRows } = await client.query(
				'SELECT * FROM pgmb.read_from_queue($1, $2)', [queue, msgsToSend]
			)
			expect(queueRows.length).toBe(msgIds.length)
			for(const [i, { id, message }] of queueRows.entries()) {
				expect(id).toBe(msgIds[i].id)
				expect(message).toEqual(msgIds[i].msg)
			}
		}
	})

	testWithRollback('should create a queue with bindings', async client => {
		const queueName = createQueueName()
		const bindings = [createExchangeName(), createExchangeName()]
		await client.query(
			'SELECT pgmb.assert_queue($1, bindings => ARRAY[$2,$3]::varchar[])',
			[queueName, ...bindings]
		)

		const { rows } = await client.query(
			'SELECT * FROM pgmb.queues WHERE name = $1', [queueName]
		)
		expect(rows.length).toBe(1)
		expect(rows[0].name).toBe(queueName)

		for(const binding of bindings) {
			const { rows: bindingRows } = await client.query(
				'SELECT * FROM pgmb.exchanges WHERE name = $1', [binding]
			)
			expect(bindingRows.length).toBe(1)
		}

		// should update bindings when re-asserted
		const newBinding = createExchangeName()
		await client.query(
			'SELECT pgmb.assert_queue($1, bindings => ARRAY[$2]::varchar[])',
			[queueName, newBinding]
		)
		const { rows: boundExchanges } = await client.query(
			'SELECT * FROM pgmb.exchanges WHERE $1 = ANY(queues)', [queueName]
		)
		expect(boundExchanges.length).toBe(1)
		expect(boundExchanges[0].name).toBe(newBinding)
	})

	function testWithRollback(
		name: string,
		fn: (client: PoolClient) => Promise<void>
	) {
		return it(name, async() => {
			const client = await pool.connect()
			await client.query('BEGIN')
			try {
				await fn(client)
			} finally {
				await client.query('ROLLBACK')
				client.release()
			}
		})
	}
})

function createQueueName() {
	const queueName = chance.word({ length: 10 }) + '_queue'
	return queueName
}

function createExchangeName() {
	const exchangeName = chance.word({ length: 10 }) + '_exchange'
	return exchangeName
}