import type { Logger } from 'pino'

export type BenchmarkConsumer = {
	queueName: string
	onMessage: (msgs: unknown[]) => Promise<void>
}

export type BenchmarkClientOpts = {
	assertQueues: string[]
	batchSize: number
	publishers: number
	consumers: BenchmarkConsumer[]
	logger: Logger
}

export type BenchmarkClient = {
	publishers: {
		publish: (queueName: string, msgs: Uint8Array[]) => Promise<void>
	}[]
	close(): Promise<void>
}

export type MakeBenchmarkClient = (opts: BenchmarkClientOpts) => (
	Promise<BenchmarkClient>
)