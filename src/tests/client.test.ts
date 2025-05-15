import { Pool } from 'pg'
import P from 'pino'
import { PGMBClient } from '../client'
import { PgIncomingMessage } from '../types'
import { delay } from '../utils'

const LOGGER = P({ level: 'trace' })
const QUEUES = [
	'test_queue',
]
const ON_MESSAGE_MOCK = jest.fn<void, [string, PgIncomingMessage[]]>()

describe('Client Tests', () => {

	const pool = new Pool({ connectionString: process.env.PG_URI, max: 1 })
	const client = new PGMBClient({
		pool,
		logger: LOGGER,
	})

	beforeAll(async() => {
		for(const queueName of QUEUES) {
			await client.assertQueue({ name: queueName })
			await client.purgeQueue(queueName)
		}

		await client.listen()
	})

	beforeEach(async() => {
		await client.replaceSubscriptions(
			...QUEUES.map(queueName => ({
				queueName,
				onMessage: ON_MESSAGE_MOCK,
				batchSize: 10
			}))
		)
	})

	afterAll(async() => {
		await client.close()
		await pool.end()
	})

	beforeEach(() => {
		ON_MESSAGE_MOCK.mockClear()
	})

	it('should consume messages', async() => {
		await client.sendToQueue(
			QUEUES[0],
			{ message: 'test message' },
			{ message: 'test message 2' }
		)

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		// batched consumption
		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
		ON_MESSAGE_MOCK.mockClear()

		// let's send another message -- ensure the listener is still
		// working.
		await client.sendToQueue(QUEUES[0], { message: 'test message 3' })

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
	})

	it('should debounce consumption', async() => {
		const queueName = QUEUES[0]

		await client.replaceSubscriptions(
			{
				queueName,
				onMessage: ON_MESSAGE_MOCK,
				batchSize: 10,
				debounceIntervalMs: 500
			}
		)
		await client.sendToQueue(QUEUES[0], { message: 'test message' })
		await delay(50)
		await client.sendToQueue(QUEUES[0], { message: 'test message 2' })

		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
	})

	it('should handle connection errors', async() => {
		const conn = await pool.connect()
		conn.release()
		ON_MESSAGE_MOCK.mockImplementationOnce(async() => {
			conn.query('SELECT pg_terminate_backend(pg_backend_pid())')
				.catch(() => { })
			await delay(100)
		})

		await client.sendToQueue(QUEUES[0], { message: 'test message' })

		await delay(1000)
		// once for error, once for success
		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(2)

		// try sending another message & check it's consumed
		ON_MESSAGE_MOCK.mockClear()
		await client.sendToQueue(QUEUES[0], { message: 'test message 2' })
		while(!ON_MESSAGE_MOCK.mock.calls.length) {
			await delay(100)
		}

		expect(ON_MESSAGE_MOCK).toHaveBeenCalledTimes(1)
	})
})