import { exec as execCallback } from 'child_process'
import { stat, writeFile } from 'fs/promises'
import { Client, Pool } from 'pg'
import { promisify } from 'util'
import { delay } from '../utils.ts'
import type { BenchmarkConsumer, MakeBenchmarkClient } from './types.ts'

const exec = promisify(execCallback)

const makePgmqBenchmarkClient: MakeBenchmarkClient = async({
	batchSize,
	consumers,
	publishers,
	assertQueues,
}) => {
	const uri = process.env.PG_URI
	if(!uri) {
		throw new Error('PG_URI is not set')
	}

	const poolSize = Math.max(1, publishers, consumers.length) + 1
	const pool = new Pool({
		max: poolSize,
		connectionString: uri,
	})

	for(const queueName of assertQueues) {
		await pool.query('SELECT pgmq.create($1);', [queueName])
	}

	const subs = consumers.map(createSubLoop)

	return {
		async close() {
			await pool.end()
			for(const sub of subs) {
				await sub.close()
			}
		},
		publishers: Array.from({ length: publishers }, () => ({
			async publish(queueName, msgs) {
				const params: unknown[] = [queueName]
				const sqlterms: string[] = []
				for(const msg of msgs) {
					params.push(`{"msg":"${Buffer.from(msg).toString('base64url')}"}`)
					sqlterms.push(`$${params.length}::jsonb`)
				}

				const sql = `SELECT pgmq.send_batch($1, ARRAY[${sqlterms.join(',')}]::jsonb[]);`
				await pool.query(sql, params)
			},
		})),
	}

	function createSubLoop({ queueName, onMessage }: BenchmarkConsumer) {
		let cancelled = false
		run()

		return {
			close() {
				cancelled = true
			}
		}

		async function run() {
			while(!cancelled) {
				const client = await pool.connect()
				await client.query('BEGIN;')
				const { rows } = await client.query(
					'SELECT * from pgmq.read($1, 30, $2);',
					[queueName, batchSize]
				)
				if(rows.length) {
					await onMessage(rows.map(r => r['message']))

					await client.query(
						'SELECT pgmq.delete($1, $2::bigint[]);',
						[queueName, `{${rows.map(r => r['msg_id']).join(',')}}`]
					)
				}

				await client.query('COMMIT;')
				await client.release()
				// keep reading until we get no more messages
				if(!rows.length) {
					await delay(100)
				}
			}
		}
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
		+ " WHERE schema_name = 'pgmq'"
	)
	if(rowCount) {
		console.log('pgmb schema already exists')
		await conn.end()
		return false
	}

	const fileExists = await stat('./sql/pgmq.sql')
		.catch(() => false)
	if(!fileExists) {
		await downloadPgmqSql()
	}

	const { stdout, stderr } = await exec(`psql ${uri} -f ./sql/pgmq.sql`)
	process.stdout.write(stdout)
	process.stderr.write(stderr)

	return true
}

// download pgmq.sql from the git repository
async function downloadPgmqSql() {
	const res = await fetch(
		'https://raw.githubusercontent.com/pgmq/pgmq/refs/heads/main/pgmq-extension/sql/pgmq.sql'
	)
	await writeFile('./sql/pgmq.sql', await res.text())
	console.log('pgmq.sql downloaded')
}

export default makePgmqBenchmarkClient