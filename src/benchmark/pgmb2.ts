import { exec } from 'child_process'
import { Client, Pool } from 'pg'
import { createSubscription, maintainEventsTable, pollForEvents, readNextEventsText, writeEvents } from '../queries.ts'
import type { BenchmarkConsumer, MakeBenchmarkClient } from './types.ts'

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

	const poolSize = Math.max(5, publishers, consumers.length)
	const pool = new Pool({ max: poolSize, connectionString: uri })
	const onMessageMap:
		{ [subscriptionId: string]: BenchmarkConsumer['onMessage'] } = {}

	await maintainEventsTable.run(undefined, pool)
	const maintaintask = publishers ?
		setInterval(
			async() => {
				await maintainEventsTable.run(undefined, pool)
				console.log('Maintained events table')
			},
			60_000
		)
		: undefined

	let polling = false
	const pollTask = consumers.length
		? setInterval(
			async() => {
				if(polling) {
					return
				}

				polling = true
				const [{ count }] = await pollForEvents.run(undefined, pool)
				console.log(`Polled for events, found ${count}`)
				polling = false
			},
			250
		)
		: undefined

	for(const name of assertQueues) {
		await createSubscription.run({
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
			await Promise.all(
				Object.entries(onMessageMap).map(async([subscriptionId, onMessage]) => {
					const rows = await readNextEventsText.run(
						{
							fetchId: subscriptionId,
							chunkSize: batchSize,
						},
						pool
					)

					onMessage(rows.map(r => r.payload))
				})
			)
		}
	}

	const task = consumers.length ? run() : undefined

	return {
		async close() {
			closed = true
			await task
			await pool.end()
			clearInterval(pollTask)
			clearInterval(maintaintask)
		},
		publishers: Array.from({ length: publishers }, () => ({
			async publish(queueName, msgs) {
				const payloads: string[] = []
				const metadatas: (string | null)[] = []
				const topics: string[] = []
				for(const msg of msgs) {
					payloads.push(`{"data":"${Buffer.from(msg.buffer, msg.byteOffset, msg.byteLength).toString('base64')}"}`)
					metadatas.push(null)
					topics.push(queueName)
				}

				await writeEvents.run({ payloads, topics, metadatas }, pool)
			},
		})),
	}
}

export async function install(fresh?: boolean) {
	const uri = process.env.PG_URI
	if(!uri) {
		throw new Error('PG_URI is not set')
	}

	const conn = new Client({ connectionString: uri })
	await conn.connect()

	if(fresh) {
		await conn.query('DROP SCHEMA IF EXISTS pgmb2 CASCADE')
	}

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