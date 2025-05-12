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
	const uri = 'postgres://postgres:@localhost:5432'
	const poolSize = Math.max(1, publishers, consumers.length) + 1
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
			async onMessage(msgs) {
				await onMessage(msgs.map(m => m.message))
			},
		})),
	})

	for(const queueName of assertQueues) {
		await client.assertQueue(queueName)
	}

	await client.open()

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