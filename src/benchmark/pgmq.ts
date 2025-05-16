import { exec } from 'child_process'
import { Client, Pool } from 'pg'
import { BenchmarkConsumer, MakeBenchmarkClient } from './types'

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
				await new Promise(r => setTimeout(r, 100))
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

	await new Promise<void>((resolve, reject) => {
		exec(`psql ${uri} -f ./sql/pgmq.sql`, (err, stdout, stderr) => {
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

export default makePgmqBenchmarkClient