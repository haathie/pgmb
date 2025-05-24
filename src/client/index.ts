import { Pool, type PoolClient } from 'pg'
import P, { type Logger } from 'pino'
import type { DefaultDataMap, PgEnqueueMsg, PGMBAssertExchangeOpts, PGMBAssertQueueOpts, PGMBClientOpts, PGMBNotification, PgPublishMsg, PGSentMessage, Serialiser } from '../types'
import { serialisePgMsgConstructorsIntoSql } from '../utils'
import { PGMBEventBatcher } from './batcher'
import { PGMBConsumer } from './consumer'
import { PGMBListener } from './listener'

export class PGMBClient<QM = DefaultDataMap, EM = DefaultDataMap> {

	#pool: Pool
	#consumers: PGMBConsumer<keyof QM, EM, QM[keyof QM]>[]
	#logger: Logger
	#listener: PGMBListener
	#serialiser?: Serialiser
	#batcherOpts: PGMBClientOpts<QM, EM>['batcher']
	#createdPool = false

	defaultBatcher: PGMBEventBatcher<EM>

	constructor({
		pool,
		logger = P(),
		consumers = [],
		serialiser,
		batcher
	}: PGMBClientOpts<QM, EM>) {
		if('create' in pool && pool.create) {
			this.#pool = new Pool(pool)
			this.#createdPool = true
		} else if(pool instanceof Pool) {
			this.#pool = pool
		} else {
			throw new Error('Either a pool or pool options must be provided')
		}

		this.#logger = logger
		this.#onNotification = this.#onNotification.bind(this)
		this.#listener
			= new PGMBListener(this.#pool, this.#onNotification, this.#logger)
		this.#serialiser = serialiser
		this.#consumers = this.#createConsumers(consumers)
		this.#batcherOpts = batcher
		this.defaultBatcher = this.createBatcher()
	}

	async listen() {
		await this.#listener.close()

		const queueOpts = this.#consumers.map(s => s.getOpts())
		if(!queueOpts.length) {
			return
		}

		const client = await this.#pool.connect()
		try {
			await client.query('BEGIN')
			for(const q of queueOpts) {
				await this.assertQueue(q, client)
			}

			await client.query('COMMIT')
		} catch(err) {
			await client.query('ROLLBACK')
			throw err
		} finally {
			client.release()
		}

		await this.#listener.subscribe(...queueOpts.map(q => String(q.name)))
		this.#logger.info({ queues: queueOpts.length }, 'listening to queues')
	}

	/**
	 * Stops listening to all queues, waits for all consumers to finish
	 * processing messages, and stops any further processing of messages.
	 */
	async close() {
		await this.defaultBatcher.close()
		await this.#listener?.close()
		await Promise.all(this.#consumers.map(s => s.close()))
		if(this.#createdPool) {
			await this.#pool.end()
		}

		this.#logger.debug('closed PGMB client')
	}

	async assertQueue(
		{
			name,
			ackSetting,
			defaultHeaders,
			type,
			bindings,
		}: PGMBAssertQueueOpts<keyof QM, keyof EM>,
		client: PoolClient | Pool = this.#pool
	) {
		let sql = 'SELECT pgmb.assert_queue(queue_name => $1'
		const params: unknown[] = [name]
		if(ackSetting) {
			params.push(ackSetting)
			sql += `, ack_setting => $${params.length}`
		}

		if(defaultHeaders) {
			params.push(JSON.stringify(defaultHeaders))
			sql += `, default_headers => ${params.length}`
		}

		if(type) {
			params.push(type)
			sql += `, type => $${params.length}`
		}

		if(bindings) {
			params.push(`{${bindings.map(b => `${String(b)}`).join(',')}}`)
			sql += `, bindings => $${params.length}::varchar[]`
		}

		sql += ') AS "created"'
		const { rows: [{ created }] } = await client.query(sql, params)
		this.#logger.debug({ name, created }, 'asserted queue')
		return created as boolean
	}

	async deleteQueue<Q extends keyof QM>(name: Q) {
		await this.#pool.query('SELECT pgmb.delete_queue($1)', [name])
		this.#logger.debug({ name }, 'deleted queue')
	}

	async purgeQueue<Q extends keyof QM>(name: Q) {
		await this.#pool.query('SELECT pgmb.purge_queue($1)', [name])
	}

	async send<Q extends keyof QM>(
		queueName: Q,
		...messages: PgEnqueueMsg<QM[Q]>[]
	) {
		if(!messages.length) {
			return []
		}

		const [arraySql, params] = serialisePgMsgConstructorsIntoSql(
			this.#serialiseMsgs(messages),
			[queueName]
		)
		const { rows } = await this.#pool
			.query(`SELECT pgmb.send($1, ${arraySql}) AS id`, params)
		return rows as PGSentMessage[]
	}

	async assertExchange<E extends keyof EM>({ name }: PGMBAssertExchangeOpts<E>) {
		await this.#pool.query('SELECT pgmb.assert_exchange($1)', [name])
		this.#logger.debug({ name }, 'asserted exchange')
	}

	async deleteExchange<E extends keyof EM>(name: E) {
		await this.#pool.query('SELECT pgmb.delete_exchange($1)', [name])
		this.#logger.debug({ name }, 'deleted exchange')
	}

	async bindQueue<
		Q extends keyof QM,
		E extends keyof EM
	>(queueName: Q, exchangeName: E) {
		await this.#pool.query(
			'SELECT pgmb.bind_queue($1, $2)',
			[queueName, exchangeName]
		)
	}

	async unbindQueue<
		Q extends keyof QM,
		E extends keyof EM
	>(queueName: Q, exchangeName: E) {
		await this.#pool.query(
			'SELECT pgmb.unbind_queue($1, $2)',
			[queueName, exchangeName]
		)
	}

	async publish(...messages: PgPublishMsg<EM>[]) {
		if(!messages.length) {
			return []
		}

		const [arraySql, params] = serialisePgMsgConstructorsIntoSql(
			this.#serialiseMsgs(messages),
			[]
		)
		const { rows } = await this.#pool
			.query(`SELECT pgmb.publish(${arraySql}) AS id`, params)
		return rows as PGSentMessage[]
	}

	/**
	 * Create a new batcher for publishing messages.
	 */
	createBatcher() {
		return new PGMBEventBatcher<EM>({
			publish: this.publish.bind(this),
			logger: this.#logger.child({ batcher: true }),
			...this.#batcherOpts
		})
	}

	#onNotification = (notif: PGMBNotification) => {
		if(notif.type === 'message') {
			for(const sub of this.#consumers) {
				if(sub.getOpts().name === notif.queueName) {
					sub.onMessage(notif.data.count)
				}
			}
		} else {
			Promise.all(this.#consumers.map(s => s.consume()))
		}
	}

	#createConsumers(opts: PGMBClientOpts<QM, EM>['consumers']) {
		return opts.flatMap(s => (
			Array.from({ length: s.replicas || 1 }, (_, i) => (
				new PGMBConsumer<keyof QM, EM, QM[keyof QM]>(
					this.#pool,
					s,
					this.#serialiser,
					this.#logger.child({ queue: s.name, r: i }),
				)
			))
		))
	}

	#serialiseMsgs<T extends PgEnqueueMsg<unknown>>(msgs: T[]) {
		type SerialisedMsg = Omit<T, 'message'> & Pick<PgEnqueueMsg, 'message'>
		if(!this.#serialiser) {
			return msgs as SerialisedMsg[]
		}

		const serialised: SerialisedMsg[] = []
		for(const msg of msgs) {
			serialised.push({
				...msg,
				headers: { ...msg.headers, contentType: this.#serialiser.contentType },
				message: this.#serialiser.encode(msg.message)
			})
		}

		return serialised
	}
}