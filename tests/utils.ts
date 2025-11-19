import type { PoolClient } from 'pg'
import type { PgEnqueueMsg } from '../src/types.ts'
import { serialisePgMsgConstructorsIntoSql } from '../src/utils.ts'

export async function send(
	client: PoolClient, queueName: string, msgs: PgEnqueueMsg[]
) {
	const [sql, params] = serialisePgMsgConstructorsIntoSql(msgs, [queueName])
	const { rows } = await client.query(
		`SELECT pgmb.send($1, ${sql}) AS id`, params
	)
	return rows as { id: string }[]
}

export async function isQueueLogged(
	client: PoolClient, queueName: string
) {
	// https://stackoverflow.com/a/29900169
	const { rows: [{ relpersistence }] } = await client.query(
		`SELECT relpersistence FROM pg_class
		where oid = $1::regclass::oid`,
		[getQueueSchemaName(queueName) + '.live_messages']
	)
	return relpersistence === 'p'
}

// util fn for testing. Do not use in production, fetch
// the schema name from the queue table instead
export function getQueueSchemaName(queueName: string) {
	return `pgmb_q_${queueName}`
}