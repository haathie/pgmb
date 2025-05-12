import { Pool } from 'pg'
import { BenchmarkConsumer, MakeBenchmarkClient } from './types'

const makePgmqBenchmarkClient: MakeBenchmarkClient = async({
	batchSize,
	consumers,
	publishers,
	assertQueues,
}) => {
	const uri = 'postgres://postgres:@localhost:5432'
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

export default makePgmqBenchmarkClient