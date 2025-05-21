import type { Pool } from 'pg'
import type { Logger } from 'pino'

export type DefaultDataMap = { [_: string]: Uint8Array | string }

/**
 * Declare the options for the PGMBClient.
 *
 * Note: if you do not provide a serialiser, the client will
 * use no serialisation & your consumers will receive the raw
 * messages.
 *
 * @template QM - Map of queue names to message types.
 * @template EM - Map of exchange names to message types.
 */
export type PGMBClientOpts<QM = DefaultDataMap, EM = DefaultDataMap> = {
	/**
	 * Provide a connection pool to use.
	 */
	pool: Pool

	logger?: Logger
} & (
	{
		/**
		 * Add consumers to the client. This will automatically
		 * start consuming messages.
		 */
		consumers: PGMBConsumerOpts[]
		serialiser?: undefined
	} | {
		/**
		 * Add consumers to the client. This will automatically
		 * start consuming messages.
		 */
		consumers: {
			[Key in keyof QM]: PGMBConsumerOpts<Key, EM, QM[Key]>
		}[keyof QM][]
		serialiser: Serialiser
	}
)

export type PGMBConsumerOpts<
	Q = string, M = DefaultDataMap, Default = Uint8Array
> = PGMBAssertQueueOpts<Q, keyof M> & {
	/**
	 * Number of messages to consume at once.
	 */
	batchSize: number
	/**
	 * Number of milliseconds to wait after receiving a message
	 * before processing the next batch. This is useful to
	 * process messages in batches
	 */
	debounceIntervalMs?: number
	/**
	 * Process messages in the queue. Rejecting a message will
	 * automatically nack it, resolving it will ack it.
	 */
	onMessage(
		queueName: Q,
		msgs: PgTypedIncomingMessage<M, Default>[]
	): Promise<void> | void
}

export type PgTypedIncomingMessage<M, D> = {
	[key in keyof M]: PgIncomingMessage<key, M[key], D>
}[keyof M]

export type PGMBMetadata<E = string> = {
	/**
	 * Specifys the number of times this message should be retried.
	 * Each element in the array is a retry delay in seconds.
	 * Eg. [5, 10, 15] will retry the message after 5 seconds,
	 * then 10 seconds, then 15 seconds.
	 */
	retriesLeftS?: number[]
	/**
	 * If this message is a retried message, this will be the ID of the
	 * original n-acked message.
	 */
	originalMessageId?: string
	/**
	 * The number of times this message has been retried.
	 */
	tries?: number
	/**
	 * The exchange this message came from
	 */
	exchange?: E
}

export type PGMBHeaders = PGMBMetadata & { [key: string]: any }

export type PgIncomingMessage<E = string, M = Uint8Array, D = Uint8Array> = {
	id: string
	/**
	 * the raw serialised message
	 */
	rawMessage: Uint8Array
	headers: PGMBHeaders
} & (
	{
		exchange: E
		message: M
	} | {
		message: D
		exchange?: undefined
	}
)

export type PgEnqueueMsg<M = Uint8Array | string> = {
	message: M
	headers?: PGMBHeaders
	consumeAt?: Date
}

export type PgPublishMsg<M = DefaultDataMap, E extends keyof M = keyof M> = {
	[Key in E]: {
		/**
		 * Exchange name to send the message to.
		 */
		exchange: Key
		message: M[Key]
		headers?: PGMBHeaders
		consumeAt?: Date
	}
}[E]

export type PGMBNotificationData = { count: number }

export type PGMBNotification = {
	type: 'message'
	queueName: string
	data: PGMBNotificationData
} | {
	type: 'connection'
}

export type PGMBAssertQueueOpts<Q = string, B = string> = {
	/***
	 * Name of the queue to assert.
	 */
	name: Q
	ackSetting?: 'archive' | 'delete'
	defaultHeaders?: PGMBHeaders
	type?: 'logged' | 'unlogged'
	bindings?: B[]
}

export type PGMBAssertExchangeOpts<T = string> = {
	name: T
}

export type PGSentMessage = { id: string }

export type Serialiser = {
	id: string
	contentType: string | undefined
	encode: (msg: unknown) => Uint8Array | string
	decode: (msg: Uint8Array) => unknown
}