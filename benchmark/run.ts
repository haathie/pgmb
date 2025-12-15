import { threadId, Worker, workerData } from 'worker_threads'
import makeAmqpBenchmarkClient from './amqp.ts'
import { benchmarkConsumption, benchmarkPublishing } from './base.ts'
import makePgmbBenchmarkClient, { install as installPgmb } from './pgmb.ts'
import makePgmb2BenchmarkClient, { install as installPgmb2 } from './pgmb.ts'
import makePgmqBenchmarkClient, { install as installPgmq } from './pgmq.ts'
import type { MakeBenchmarkClient } from './types.ts'

type Client = {
	make: MakeBenchmarkClient
	install?: (fresh?: boolean) => Promise<boolean>
}

const FILENAME = process.argv[1]

const CLIENTS: { [client: string]: Client } = {
	'pgmb': {
		make: makePgmbBenchmarkClient,
		install: installPgmb
	},
	'pgmb': {
		make: makePgmb2BenchmarkClient,
		install: installPgmb2
	},
	'pgmq': {
		make: makePgmqBenchmarkClient,
		install: installPgmq
	},
	'amqp': {
		make: makeAmqpBenchmarkClient,
	}
}

function getArg(name: string) {
	const index = process.argv.indexOf('--' + name)
	if(index === -1) {
		return undefined
	}

	const value = process.argv[index + 1]
	if(!value) {
		return null
	}

	return value
}

if(!workerData) {
	const clientId = getArg('client')
	if(!clientId) {
		throw new Error('Please specify --client <client>')
	}

	const methodArg = getArg('consume')
		? 'consume'
		: (getArg('publish') ? 'publish' : null)
	const methods = methodArg ? [methodArg] : ['consume', 'publish']

	const fresh = !!getArg('fresh')

	const batchSize = getArg('batch')

	const replicas = getArg('replicas') || '1'

	const queues = getArg('queues') || '1'
	const testQueues = [...Array.from({ length: Number(queues) })]
		.flatMap((_, i) => (
			[...Array.from({ length: Number(replicas) })]
				.map(() => `test_queue_${i}`)
		))
	const runWorkers = () => (
		testQueues.flatMap(queueName => (
			methods.map(method => (
				new Worker(FILENAME, {
					workerData: {
						queueName,
						method,
						clientId,
						batchSize: batchSize ? Number(batchSize) : undefined,
					}
				})
			))
		))
	)

	const install = CLIENTS[clientId]?.install
	if(install) {
		if(fresh) {
			console.log(`Installing fresh client ${clientId}...`)
		}

		install(fresh)
			.then(installed => {
				console.log(
					installed
						? `Client ${clientId} installed successfully`
						: `Client ${clientId} already installed`
				)

				return runWorkers()
			})
			.catch(err => {
				console.error('Error installing client', err)
				process.exit(1)
			})
	} else {
		runWorkers()
	}
} else {
	const { queueName, clientId, method, batchSize } = workerData
	const client = CLIENTS[clientId]
	if(!client) {
		throw new Error(
			`Client ${clientId} not found. `
			+ `Available clients: ${Object.keys(CLIENTS).join(', ')}`
		)
	}

	const opts = {
		makeClient: client.make,
		id: `${clientId}-${threadId}`,
		queueName,
		batchSize,
	}

	if(method === 'consume') {
		benchmarkConsumption(opts)
	} else if(method === 'publish') {
		benchmarkPublishing(opts)
	} else {
		throw new Error('INTERNAL: unknown method ' + method)
	}
}
