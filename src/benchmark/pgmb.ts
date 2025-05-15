import { Pool } from 'pg'
import { PGMBClient } from '../client'
import { MakeBenchmarkClient } from './types'

const makePgmbBenchmarkClient: MakeBenchmarkClient = async({
	batchSize,
	consumers,
	publishers,
	assertQueues,
	logger
}) => {
	const uri = process.env.PG_URI
	if(!uri) {
		throw new Error('PG_URI is not set')
	}

	const poolSize = Math.max(1, publishers, consumers.length)
	const pool = new Pool({
		max: poolSize,
		connectionString: uri,
	})
	const client = new PGMBClient({
		pool,
		logger,
		consumers: consumers.map(({ queueName, onMessage }) => ({
			queueName,
			batchSize,
			async onMessage(_, msgs) {
				await onMessage(msgs.map(m => m.message))
			},
		})),
	})

	for(const name of assertQueues) {
		await client.assertQueue({ name })
	}

	await client.listen()

	return {
		async close() {
			await client.close()
			await pool.end()
		},
		publishers: Array.from({ length: publishers }, () => ({
			async publish(queueName, msgs) {
				await client.sendToQueue(
					queueName,
					...msgs.map(m => ({ message: m })),
				)
			},
		})),
	}
}

export default makePgmbBenchmarkClient