import { exec } from 'child_process'
import { Client, Pool } from 'pg'
import { createReader, createSubscription, readNextEventsForSubscriptionsText, readNextEventsText, writeEvents } from '../queries.ts'
import { delay } from '../utils.ts'
import type { MakeBenchmarkClient } from './types.ts'

const READER_ID = 'benchmark'

const makePgmb2BenchmarkClient: MakeBenchmarkClient = async({
	batchSize,
	consumers,
	publishers,
	assertQueues,
}) => {
	const uri = process.env.PG_URI
	if(!uri) {
		throw new Error('PG_URI is not set')
	}

	const poolSize = Math.max(1, publishers, consumers.length)
	const pool = new Pool({ max: poolSize, connectionString: uri })
	const onMessageMap:
		{ [subscriptionId: string]: (msgs: Uint8Array[]) => Promise<void> } = {}

	try {
		await createReader.run({ readerId: READER_ID }, pool)
	} catch(err: any) {
		// Reader already exists
		if(err.code !== '23505') {
			throw err
		}
	}

	for(const name of assertQueues) {
		await createSubscription.run({
			readerId: READER_ID,
			id: name,
			conditionsSql: 'e.topic = s.id',
		}, pool)
	}

	for(const { queueName, onMessage } of consumers) {
		onMessageMap[queueName] = onMessage
	}

	let closed = false
	const run = async() => {
		while(!closed) {
			const events = await readNextEventsForSubscriptionsText
				.run({ readerId: READER_ID, chunkSize: batchSize }, pool)

			const subIdPayloadMap: { [subscriptionId: string]: Uint8Array[] } = {}
			for(const { topic, payload } of events) {
				subIdPayloadMap[topic] ||= []
				subIdPayloadMap[topic].push(payload as Uint8Array)
			}

			await Promise.all(
				Object.entries(subIdPayloadMap)
					.map(([subId, payloads]) => onMessageMap[subId]?.(payloads))
			)

			// await delay(100)
		}
	}

	const task = consumers.length ? run() : undefined

	return {
		async close() {
			closed = true
			await task
			await pool.end()
		},
		publishers: Array.from({ length: publishers }, () => ({
			async publish(queueName, msgs) {
				await writeEvents.run(
					{
						payloads: msgs.map(m => `{"data":"${Buffer.from(m.buffer, m.byteOffset, m.byteLength).toString('base64')}"}`),
						topics: msgs.map(() => queueName),
						metadatas: msgs.map(() => null),
					},
					pool,
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
		+ " WHERE schema_name = 'pgmb2'"
	)
	if(rowCount) {
		await conn.end()
		return false
	}

	await new Promise<void>((resolve, reject) => {
		exec(`psql ${uri} -f ./sql/pgmb2.sql -1`, (err, stdout, stderr) => {
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

export default makePgmb2BenchmarkClient