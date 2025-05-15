import { randomBytes } from 'crypto'
import P from 'pino'
import { MakeBenchmarkClient } from './types'

const MSG_SIZE_BYTES = 1024
const LOGGER = P({ })
const CONSUMPTION_CONCURRENCY = 10
const PUBLISH_CONCURRENCY = 5
const BATCH_SIZE = 100

type BenchmarkOpts = {
	makeClient: MakeBenchmarkClient
	id: string
	queueName: string
}

export async function benchmarkConsumption({
	makeClient,
	id,
	queueName
}: BenchmarkOpts) {
	let consumed = 0
	let totalConsumed = 0
	const logger = LOGGER.child({ cnm: 1, id, queueName })
	const int = setInterval(async() => {
		logger.info({ value: consumed, total: totalConsumed }, 'metrics')
		consumed = 0
	}, 10_000)
	const totalConcurrency = CONSUMPTION_CONCURRENCY

	const client = await makeClient({
		assertQueues: [queueName],
		batchSize: BATCH_SIZE,
		consumers: Array.from({ length: totalConcurrency }, (_, i) => ({
			queueName: queueName,
			async onMessage(msgs) {
				consumed += msgs.length
				totalConsumed += msgs.length
				logger.info(
					{ client: i, count: msgs.length, totalConsumed },
					'consumed messages'
				)
				await new Promise(resolve => setTimeout(resolve, 25))
			},
		})),
		publishers: 0,
		logger,
	})

	process.on('SIGINT', async() => {
		clearInterval(int)
		await client.close()
		process.exit(0)
	})
}

export async function benchmarkPublishing({
	queueName,
	makeClient,
	id
}: BenchmarkOpts) {
	let published = 0
	let totalPublished = 0
	let killed = false

	const logger = LOGGER.child({ pub: 1, id, queueName })

	const int = setInterval(async() => {
		logger.info({ value: published, total: totalPublished }, 'metrics')
		published = 0
	}, 10_000)

	const client = await makeClient({
		assertQueues: [queueName],
		batchSize: 100,
		publishers: PUBLISH_CONCURRENCY,
		consumers: [],
		logger,
	})

	process.on('SIGINT', () => {
		killed = true
		logger.info('shutting down...')
		logger.flush()
	})

	await Promise.all(client.publishers.map(async(pub, i) => {
		while(!killed) {
			const count = Math.floor(Math.random() * 100) + 100

			const msgs = Array.from({ length: count }, () => (
				randomBytes(MSG_SIZE_BYTES)
			))
			await pub.publish(queueName, msgs)
			totalPublished += count
			published += count
			logger.info({ client: i, count, totalPublished }, 'published message')
		}
	}))

	clearInterval(int)
	await client.close()
}