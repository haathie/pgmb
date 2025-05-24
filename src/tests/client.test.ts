import assert from 'assert'
import { Pool } from 'pg'
import P from 'pino'
import { PGMBClient } from '../client'
import { V8Serialiser } from '../serialisers/v8'
import { PGMBClientOpts, PGMBOnMessageOpts } from '../types'
import { delay } from '../utils'
import { getQueueSchemaName, send } from './utils'

type QueueTypeMap = {
	'test_queue_1': { msg: string }
	'test_queue_2': { msg: string }
}

type ExchangeTypeMap = {
	'test_exchange_1': { a: string }
	'test_exchange_2': { b: string }
}

type ClientOpts = PGMBClientOpts<QueueTypeMap, ExchangeTypeMap>

const LOGGER = P({ level: 'trace' })
const IDLE_TIMEOUT_MS = 2000
const MAX_BATCH_SIZE = 5

const ON_MESSAGE_MOCK = jest.fn<
	void, [
		PGMBOnMessageOpts<
			keyof QueueTypeMap,
			ExchangeTypeMap,
			QueueTypeMap[keyof QueueTypeMap]
		>
	]
>()

describe('Client Tests', () => {

	const pool = new Pool({
		connectionString: process.env.PG_URI,
		max: 1,
		idleTimeoutMillis: IDLE_TIMEOUT_MS
	})
	const defaultQueueName = 'test_queue_1'

	let opts: ClientOpts
	let client: PGMBClient<QueueTypeMap, ExchangeTypeMap>

	beforeEach(async() => {
		opts = {
			pool,
			logger: LOGGER,
			batcher: { flushIntervalMs: 500, maxBatchSize: MAX_BATCH_SIZE },
			consumers: [
				{
					name: 'test_queue_1',
					onMessage: ON_MESSAGE_MOCK,
					batchSize: 10,
				},
				{
					name: 'test_queue_2',
					onMessage: ON_MESSAGE_MOCK,
					batchSize: 10,
				}
			],
			serialiser: V8Serialiser
		}
		client = new PGMBClient(opts)

		for(const { name } of opts.consumers) {
			await client.assertQueue({ name, bindings: [] })
			await client.purgeQueue(name)
		}

		await client.listen()

		ON_MESSAGE_MOCK.mockClear()
	})

	afterEach(async() => {
		await client.close()
	})

	afterAll(async() => {
		await pool.end()
	})

	it('should consume messages', async() => {
		const msgsToSend: QueueTypeMap[typeof defaultQueueName][] = [
			{ msg: 'test' },
			{ msg: 'test2' },
		]
		const msgs = await client.send(
			defaultQueueName,
			...msgsToSend.map(message => ({ message }))
		)
		expect(msgs).toHaveLength(2)
		// ensure it's an ID string
		expect(msgs[0].id).toContain('pm')

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		// batched consumption
		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)

		const { msgs: recvMsgs } = ON_MESSAGE_MOCK.mock.calls[0][0]
		expect(recvMsgs).toHaveLength(2)
		for(const [i, { id, message, rawMessage, headers }] of recvMsgs.entries()) {
			expect(headers.contentType).toBeTruthy()
			expect(id).toContain('pm')
			expect(rawMessage).toBeTruthy()
			expect(message).toEqual(msgsToSend[i])
		}

		ON_MESSAGE_MOCK.mockClear()

		// let's send another message -- ensure the listener is still
		// working.
		await client.send(defaultQueueName, { message: { msg: 'test 3' } })

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
	})

	it('should consume messages on multiple queues', async() => {
		const secondQueueName = 'test_queue_2'
		// we'll block the connection of the first queue, to ensure that
		// the second queue is still consumed. We're checking that the listener
		// still sends out notifications despite the first queue being blocked.
		let gotMessage = false
		ON_MESSAGE_MOCK.mockImplementationOnce(async() => {
			gotMessage = true
			await delay(1000)
		})

		await client.send(defaultQueueName, { message: { msg: '1' } })
		while(!gotMessage) {
			await delay(50)
		}

		await client.send(secondQueueName, { message: { msg: '2' } })
		while(ON_MESSAGE_MOCK.mock.calls.length < 2) {
			await delay(100)
		}

		const secondCallArgs = ON_MESSAGE_MOCK.mock.calls[1][0]
		// ensure the second call is for the second queue
		expect(secondCallArgs.queueName).toBe(secondQueueName)
	})

	it('should allow mixed ack/nack', async() => {
		const msgsToSend: QueueTypeMap[typeof defaultQueueName][] = [
			{ msg: 'test' },
			{ msg: 'test2' },
		]

		ON_MESSAGE_MOCK.mockImplementationOnce(async({ msgs, ack }) => {
			expect(msgs).toHaveLength(2)
			ack(true, msgs[0].id)
			ack(false, msgs[1].id)
		})

		const msgs = await client.send(
			defaultQueueName,
			...msgsToSend.map(message => ({ message, headers: { retriesLeftS: [5] } }))
		)
		expect(msgs).toHaveLength(2)

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		await delay(500)
		const { rows } = await pool.query(
			`SELECT * from ${getQueueSchemaName(defaultQueueName)}.live_messages`,
		)
		expect(rows).toHaveLength(1)
		expect(rows[0].headers.originalMessageId).toEqual(msgs[1].id)
	})

	it('should automatically nack failed serialisation', async() => {
		const conn = await pool.connect()
		const [, { id: successId }] = await send(
			conn,
			defaultQueueName,
			[
				{ message: 'bogus' },
				{
					message: opts.serialiser!.encode({ msg: 'test' }),
					headers: { contentType: opts.serialiser!.contentType }
				}
			]
		)
		conn.release()

		await delay(100)

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
		const { msgs } = ON_MESSAGE_MOCK.mock.calls[0][0]
		expect(msgs).toHaveLength(1)
		expect(msgs[0].id).toEqual(successId)

		// failed serialisation msg should've been nacked
		const { rows } = await pool.query(
			`SELECT * from ${getQueueSchemaName(defaultQueueName)}.live_messages`,
		)
		expect(rows).toHaveLength(0)
	})

	it('should debounce consumption', async() => {
		opts = {
			...opts,
			// required for type-checking
			serialiser: opts.serialiser!,
			consumers: [
				{
					name: defaultQueueName,
					onMessage: ON_MESSAGE_MOCK,
					batchSize: 10,
					debounceIntervalMs: 500
				},
			]
		}
		await client.close()
		client = new PGMBClient(opts)
		await client.listen()

		await client.send(defaultQueueName, { message: { msg: '1' } })
		await delay(50)
		await client.send(defaultQueueName, { message: { msg: '2' } })

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
	})

	it('should create multiple replicas', async() => {
		opts = {
			pool: { create: true, connectionString: process.env.PG_URI, max: 5 },
			logger: LOGGER,
			// required for type-checking
			serialiser: opts.serialiser!,
			consumers: [
				{
					name: defaultQueueName,
					onMessage: ON_MESSAGE_MOCK,
					batchSize: 5,
					replicas: 2,
				},
			]
		}
		await client.close()
		client = new PGMBClient(opts)
		await client.listen()

		let resolveMsg: (() => void) | undefined
		ON_MESSAGE_MOCK.mockImplementationOnce(() => {
			return new Promise<void>(r => {
				resolveMsg = r
			})
		})

		await client.send(defaultQueueName, { message: { msg: '1' } })
		while(ON_MESSAGE_MOCK.mock.calls.length < 1) {
			await delay(50)
		}

		await client.send(defaultQueueName, { message: { msg: '2' } })
		while(ON_MESSAGE_MOCK.mock.calls.length < 2) {
			await delay(50)
		}

		resolveMsg?.()
	})

	it('should handle connection errors', async() => {
		const conn = await pool.connect()
		conn.release()
		ON_MESSAGE_MOCK.mockImplementationOnce(async() => {
			conn.query('SELECT pg_terminate_backend(pg_backend_pid())')
				.catch(() => { })
			await delay(100)
		})

		await client.send(defaultQueueName, { message: { msg: '1234' } })

		await delay(1000)
		// once for error, once for success
		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(2)

		// try sending another message & check it's consumed
		ON_MESSAGE_MOCK.mockClear()
		await client.send(defaultQueueName, { message: { msg: '2345' } })
		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
	})

	it('should handle connection closures', async() => {
		// in case the pool closes the connection due to idle timeout
		// or anything else -- the listener should be able to reconnect
		await delay(IDLE_TIMEOUT_MS + 500)

		// now when we send a message, it should still be consumed
		await client.send(defaultQueueName, { message: { msg: '' } })
		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}
	})

	describe('Exchanges', () => {

		const EXCHANGE_NAME = 'test_exchange_1'
		beforeEach(async() => {
			await client.assertExchange({ name: EXCHANGE_NAME })
		})

		it('should bind queue to exchange', async() => {
			await client.bindQueue(defaultQueueName, EXCHANGE_NAME)
		})

		it('should send messages to exchange', async() => {
			await client.bindQueue(defaultQueueName, EXCHANGE_NAME)
			const pubIds = await client.publish(
				{
					exchange: EXCHANGE_NAME,
					message: { a: 'hello' }
				},
				{
					exchange: EXCHANGE_NAME,
					message: { a: 'world' }
				}
			)
			expect(pubIds).toHaveLength(2)
			// ensure it's an ID string
			expect(pubIds[0].id).toContain('pm')

			while(!ON_MESSAGE_MOCK.mock.calls.length) {
				await delay(100)
			}

			expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)

			const { msgs: recvMsgs } = ON_MESSAGE_MOCK.mock.calls[0][0]
			expect(recvMsgs).toHaveLength(2)
			for(const { id, message, exchange } of recvMsgs) {
				assert(exchange === EXCHANGE_NAME)
				// accessing "a" directly here, to ensure our types
				// are correct too. If build fails, our types may be messed up.
				expect(typeof message.a).toEqual('string')
				expect(id).toContain('pm')
			}
		})
	})

	describe('Batching', () => {

		const EXCHANGE_NAME = 'test_exchange_1'
		beforeEach(async() => {
			await client.assertQueue({
				name: defaultQueueName,
				bindings: [EXCHANGE_NAME],
			})
		})

		it('should enqueue and publish messages', async() => {
			client.defaultBatcher.enqueue({
				exchange: EXCHANGE_NAME,
				message: { a: 'hello' }
			})
			await client.defaultBatcher.flush()
			while(!ON_MESSAGE_MOCK.mock.calls.length) {
				await delay(100)
			}
		})

		it('should auto flush on batch size', async() => {
			for(let i = 0; i < MAX_BATCH_SIZE; i++) {
				client.defaultBatcher.enqueue({
					exchange: EXCHANGE_NAME,
					message: { a: 'hello ' + i }
				})
			}

			while(!ON_MESSAGE_MOCK.mock.calls.length) {
				await delay(100)
			}

			expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
		})
	})
})