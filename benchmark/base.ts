import { randomBytes } from 'crypto'
import P from 'pino'
import type { MakeBenchmarkClient } from './types.ts'

const MSG_SIZE_BYTES = 1024
const LOG_LEVEL = process.env.LOG_LEVEL || 'info'
const LOGGER = P({ level: LOG_LEVEL })
const CONSUMPTION_CONCURRENCY = 10
const PUBLISH_CONCURRENCY = 5
const BATCH_SIZE = 100

type BenchmarkOpts = {
	makeClient: MakeBenchmarkClient
	id: string
	queueName: string
	batchSize?: number
}

export async function benchmarkConsumption({
	makeClient,
	id,
	queueName,
	batchSize = BATCH_SIZE
}: BenchmarkOpts) {
	let consumed = 0
	let totalConsumed = 0
	const logger = LOGGER.child({ cnm: 1, id, queueName })
	const int = setInterval(async() => {
		console.log(`consume ${formatDt()} ${id} ${queueName} ${consumed} ${totalConsumed}`)
		consumed = 0
	}, 10_000)
	const totalConcurrency = CONSUMPTION_CONCURRENCY

	const client = await makeClient({
		assertQueues: [queueName],
		batchSize,
		consumers: Array.from({ length: totalConcurrency }, (_, i) => ({
			queueName: queueName,
			async onMessage(msgs) {
				consumed += msgs.length
				totalConsumed += msgs.length
				logger.debug(
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
	id,
	batchSize = BATCH_SIZE
}: BenchmarkOpts) {
	let published = 0
	let totalPublished = 0
	let killed = false

	const logger = LOGGER.child({ pub: 1, id, queueName })

	const int = setInterval(async() => {
		console.log(`publish ${formatDt()} ${id} ${queueName} ${published} ${totalPublished}`)
		published = 0
	}, 10_000)

	const client = await makeClient({
		assertQueues: [queueName],
		batchSize,
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
			const count = batchSize
			const msgs = Array.from({ length: count }, () => (
				randomBytes(MSG_SIZE_BYTES)
			))
			await pub.publish(queueName, msgs)
			totalPublished += count
			published += count
			logger.debug({ client: i, count, totalPublished }, 'published message')
		}
	}))

	clearInterval(int)
	await client.close()
}

function formatDt(dt: Date = new Date()) {
	const date = dt.toJSON()
	// remove milliseconds and timezone
	return date.substring(0, date.length - 5)
}