import type { Pool, PoolConfig } from 'pg'
import type { Logger } from 'pino'

export type DefaultDataMap = { [_: string]: unknown }

export type DefaultSerialisedMap = { [_: string]: Uint8Array | string }

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
export type PGMBClientOpts<QM = DefaultDataMap, EM = DefaultDataMap>
	= PGMBRawClientOpts<QM, EM> | PGMBTypedClientOpts<QM, EM>

export type PGMBRawClientOpts<QM, EM> = PGMBBaseClientOpts<EM> & {
	/**
	 * Add consumers to the client. This will automatically
	 * start consuming messages.
	 */
	consumers: PGMBConsumerOpts<keyof QM, DefaultSerialisedMap, Uint8Array>[]
	serialiser?: undefined
}

export type PGMBTypedClientOpts<QM, EM> = PGMBBaseClientOpts<EM> & {
	/**
	 * Add consumers to the client. This will automatically
	 * start consuming messages.
	 */
	consumers: {
		[Key in keyof QM]: PGMBConsumerOpts<Key, EM, QM[Key]>
	}[keyof QM][]
	serialiser: Serialiser
}

export type PGMBBaseClientOpts<EM> = {
	/**
	 * Provide a connection pool to use,
	 * or options to create a new pool.
	 */
	pool: Pool | ({ create: true } & PoolConfig)

	logger?: Logger
	/**
	 * Options for the event batcher. This can be used to queue messages
	 * before publishing them all at once.
	 */
	batcher?: Omit<PGMBMakeEventBatcherOpts<EM>, 'publish' | 'logger'>
}

export type PGMBOnMessageOpts<Q, M, Default> = {
	queueName: Q
	msgs: PgTypedIncomingMessage<M, Default>[]

	logger: Logger
	/**
	 * Mark the messages as processed.
	 * This will be transmitted to the database, upon completion
	 * of the `onMessage` function.
	 * @param success - If true, the messages will be acked,
	 * 	if false, they will be nacked.
	 */
	ack(success: boolean, ...msgs: string[]): void
}

export type PGMBConsumerOpts<Q, M, Default> = PGMBAssertQueueOpts<Q, keyof M> & {
	/**
	 * Number of replicas to create for this queue, this is useful to ensure that
	 * if you have one long running batch, other replicas can still continue
	 * consuming messages.
	 * Ensure that you've enough connections in your pool to handle this.
	 * @default 1
	 */
	replicas?: number
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
	onMessage(opts: PGMBOnMessageOpts<Q, M, Default>): Promise<void> | void
}

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
	/**
	 * type of the content
	 */
	contentType?: string
}

export type PGMBHeaders = PGMBMetadata & { [key: string]: any }

export type PgTypedIncomingMessage<M, D> = {
	[key in keyof M]: PgIncomingMessage<key, M[key], D>
}[keyof M]

type PgIncomingMessage<E = string, M = Uint8Array, D = Uint8Array> = {
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

export type PgPublishMsg<M = DefaultSerialisedMap, E extends keyof M = keyof M> = {
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

export type PGPublishedMessage = { id: string | null }

export type PGMBMakeEventBatcherOpts<M> = {
	/**
	 * Whether a particular published message should be logged.
	 * By default, all messages are logged -- in case of certain
	 * failures, the logs can be used to replay the messages.
	 */
	shouldLog?(msg: PgPublishMsg<M>): boolean

	publish(...msgs: PgPublishMsg<M>[]): Promise<PGPublishedMessage[]>

	logger: Logger
	/**
	 * @param msg - The message to serialise
	 */
	serialise?(msg: unknown): Uint8Array | string
	/**
	 * Max message size allowed, prevents enqueuing messages
	 * that are guaranteed to fail.
	 * @default 10 * 1024 * 1024 (10mb)
	 */
	maxMessageSizeBytes?: number
	/**
	 * Automatically flush after this interval.
	 * Set to undefined or 0 to disable. Will need to
	 * manually call `flush()` to publish messages.
	 * @default undefined
	 */
	flushIntervalMs?: number
	/**
	 * Max number of messages to send in a batch
	 * @default 2500
	 */
	maxBatchSize?: number
}

export type Serialiser = {
	/**
	 * Arbitrary ID for the serialiser.
	 */
	id: string
	/**
	 * Content type of the serialised message, included in the headers
	 * of the sent message.
	 * @example 'application/json'
	 */
	contentType: string | undefined
	encode: (msg: unknown) => Uint8Array | string
	decode: (msg: Uint8Array) => unknown
}