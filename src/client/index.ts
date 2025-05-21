import type { Pool, PoolClient } from 'pg'
import P, { type Logger } from 'pino'
import type { PgEnqueueMsg, PGMBAssertExchangeOpts, PGMBAssertQueueOpts, PGMBClientOpts, PGMBConsumerOpts, PGMBNotification, PgPublishMsg, PGSentMessage } from '../types'
import { serialisePgMsgConstructorsIntoSql } from '../utils'
import { PGMBConsumer } from './consumer'
import { PGMBListener } from './listener'

export class PGMBClient {

	#pool: Pool
	#consumers: PGMBConsumer[]
	#logger: Logger
	#listener: PGMBListener

	constructor({
		pool,
		logger = P(),
		consumers = [],
	}: PGMBClientOpts) {
		this.#pool = pool
		this.#consumers = consumers.map(s => (
			new PGMBConsumer(this.#pool,	s, logger.child({ queue: s.queue.name }))
		))
		this.#logger = logger
		this.#onNotification = this.#onNotification.bind(this)
		this.#listener
			= new PGMBListener(this.#pool, this.#onNotification, this.#logger)
	}

	async listen() {
		await this.#listener.close()

		const queues = this.#consumers.map(s => s.getOpts().queue)
		if(!queues.length) {
			return
		}

		const client = await this.#pool.connect()
		try {
			await client.query('BEGIN')
			for(const q of queues) {
				await this.assertQueue(q, client)
			}

			await client.query('COMMIT')
		} catch(err) {
			await client.query('ROLLBACK')
			throw err
		} finally {
			client.release()
		}

		await this.#listener.subscribe(...queues.map(q => q.name))
		this.#logger.info({ queues: queues.length }, 'listening to queues')
	}

	/**
	 * Stops consuming messages from existing consumers and replaces them with
	 * the newly provided consumers. Will automatically call `listen()` to
	 * re-subscribe to the queues.
	 */
	async replaceConsumers(...subs: PGMBConsumerOpts[]) {
		for(const cons of this.#consumers) {
			await cons.close()
		}

		this.#consumers = subs.map(s => (
			new PGMBConsumer(this.#pool, s, this.#logger.child({ queue: s.queue.name }))
		))
		await this.listen()
	}

	async close() {
		await this.#listener?.close()
		await Promise.all(
			this.#consumers.map(s => s.close())
		)
	}

	async assertQueue(
		{
			name,
			ackSetting = 'delete',
			defaultHeaders = {},
			type = 'logged',
			bindings = [],
		}: PGMBAssertQueueOpts,
		client: PoolClient | Pool = this.#pool
	) {
		const { rows: [{ created }] } = await client.query(
			'SELECT pgmb.assert_queue($1, $2, $3, $4, $5::varchar[]) AS "created"',
			[
				name, ackSetting,
				JSON.stringify(defaultHeaders), type,
				`{${bindings.map(b => `'${b}'`).join(',')}}`
			]
		)
		this.#logger.debug({ name, created }, 'asserted queue')
		return created as boolean
	}

	async deleteQueue(name: string) {
		await this.#pool.query('SELECT pgmb.delete_queue($1)', [name])
		this.#logger.debug({ name }, 'deleted queue')
	}

	async purgeQueue(name: string) {
		await this.#pool.query('SELECT pgmb.purge_queue($1)', [name])
	}

	async send(queueName: string, ...messages: PgEnqueueMsg[]) {
		if(!messages.length) {
			return []
		}

		const [arraySql, params]
			= serialisePgMsgConstructorsIntoSql(messages, [queueName])
		const { rows } = await this.#pool
			.query(`SELECT pgmb.send($1, ${arraySql}) AS id`, params)
		return rows as PGSentMessage[]
	}

	async assertExchange({ name }: PGMBAssertExchangeOpts) {
		await this.#pool.query('SELECT pgmb.assert_exchange($1)', [name])
		this.#logger.debug({ name }, 'asserted exchange')
	}

	async deleteExchange(name: string) {
		await this.#pool.query('SELECT pgmb.delete_exchange($1)', [name])
		this.#logger.debug({ name }, 'deleted exchange')
	}

	async bindQueue(queueName: string, exchangeName: string) {
		await this.#pool.query(
			'SELECT pgmb.bind_queue($1, $2)',
			[queueName, exchangeName]
		)
	}

	async unbindQueue(queueName: string, exchangeName: string) {
		await this.#pool.query(
			'SELECT pgmb.unbind_queue($1, $2)',
			[queueName, exchangeName]
		)
	}

	async publish(...messages: PgPublishMsg[]) {
		if(!messages.length) {
			return []
		}

		const [arraySql, params] = serialisePgMsgConstructorsIntoSql(messages, [])
		const { rows } = await this.#pool
			.query(`SELECT pgmb.publish(${arraySql}) AS id`, params)
		return rows as PGSentMessage[]
	}

	#onNotification = (notif: PGMBNotification) => {
		if(notif.type === 'message') {
			for(const sub of this.#consumers) {
				if(sub.getOpts().queue.name === notif.queueName) {
					sub.onMessage(notif.data.count)
				}
			}
		} else {
			Promise.all(this.#consumers.map(s => s.consume()))
		}
	}
}