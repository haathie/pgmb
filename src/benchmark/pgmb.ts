import { exec } from 'child_process'
import { Client, Pool } from 'pg'
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
				await client.send(
					queueName,
					...msgs.map(m => ({ message: m })),
				)
			},
		})),
	}
}

export async function install() {
	const uri = process.env.PG_URI
	if(!uri) {
		throw new Error('PG_URI is not set')
	}

	const conn = new Client({ connectionString: uri })
	await conn.connect()
	const { rowCount } = await conn.query(
		'SELECT schema_name FROM information_schema.schemata'
		+ " WHERE schema_name = 'pgmb'"
	)
	if(rowCount) {
		console.log('pgmb schema already exists')
		await conn.end()
		return false
	}

	await new Promise<void>((resolve, reject) => {
		exec(`psql ${uri} -f ./sql/pgmb.sql`, (err, stdout, stderr) => {
			process.stdout.write(stdout)
			process.stderr.write(stderr)
			if(err) {
				console.error(`Error: ${err.message}`)
				reject(err)
				return
			}

			resolve()
		})
	})

	return true
}

export default makePgmbBenchmarkClient