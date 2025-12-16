import assert, { AssertionError } from 'node:assert'
import type { IncomingMessage, ServerResponse } from 'node:http'
import type { PgmbClient } from './client.ts'
import type { IReplayEventsResult } from './queries.ts'
import { replayEvents } from './queries.ts'
import type { IEphemeralListener, IEvent, IEventData, SSERequestHandlerOpts } from './types.ts'
import { getCreateDateFromSubscriptionId, getDateFromMessageId } from './utils.ts'

export function createSSERequestHandler<T extends IEventData>(
	this: PgmbClient<T>,
	{
		getSubscriptionOpts,
		maxReplayEvents = 1000,
		maxReplayIntervalMs = 5 * 60 * 1000,
		jsonifier = JSON
	}: SSERequestHandlerOpts,
) {

	return handleSSERequest.bind(this)

	async function handleSSERequest(
		this: PgmbClient<T>,
		req: IncomingMessage,
		res: ServerResponse
	) {
		let sub: IEphemeralListener<T> | undefined
		let eventsToReplay: IReplayEventsResult[] = []

		try {
			assert(
				req.method?.toLowerCase() === 'get',
				'SSE only supports GET requests'
			)
			// validate last-event-id header
			const fromEventId = req.headers['last-event-id']
			if(fromEventId) {
				assert(maxReplayEvents > 0, 'replay disabled on server')
				assert(typeof fromEventId === 'string', 'invalid last-event-id header')
				const fromDt = getDateFromMessageId(fromEventId)
				assert(fromDt, 'invalid last-event-id header value')
				assert(
					fromDt.getTime() >= (Date.now() - maxReplayIntervalMs),
					'last-event-id is too old to replay'
				)
			}

			sub = await this.registerFireAndForgetSubscription({
				...await getSubscriptionOpts(req),
				expiryInterval: `${maxReplayIntervalMs * 2} milliseconds`
			})

			if(fromEventId) {
				const fromDt = getDateFromMessageId(fromEventId)!
				const subDt = getCreateDateFromSubscriptionId(sub.id)
				assert(subDt, 'internal: invalid subscription id format')
				assert(
					fromDt >= subDt,
					'last-event-id is before subscription creation, cannot replay'
				)

				eventsToReplay = await replayEvents.run(
					{
						groupId: this.groupId,
						subscriptionId: sub.id,
						fromEventId: fromEventId,
						maxEvents: maxReplayEvents
					},
					this.client
				)

				this.logger.trace(
					{ subId: sub.id, count: eventsToReplay.length },
					'got events to replay'
				)
			}

			if(res.writableEnded) {
				throw new Error('response already ended')
			}
		} catch(err) {
			this.logger
				.error({ subId: sub?.id, err }, 'error in sse subscription setup')

			await sub?.throw(err).catch(() => { })

			if(res.writableEnded) {
				return
			}

			const message = err instanceof Error ? err.message : String(err)
			// if an assertion failed, we cannot connect with these parameters
			// so use 204 No Content
			const code = err instanceof AssertionError ? 204 : 500
			res
				.writeHead(code, message)
				.end()
			return
		}

		res.once('close', () => {
			sub?.return()
		})
		res.once('error', err => {
			sub?.throw(err).catch(() => {})
		})

		res.writeHead(200, {
			'content-type': 'text/event-stream',
			'cache-control': 'no-cache',
			'connection': 'keep-alive',
			'transfer-encoding': 'chunked',
		})
		res.flushHeaders()

		try {
			// send replayed events first
			writeSseEvents(res, eventsToReplay as IEvent<T>[])

			for await (const { items } of sub) {
				writeSseEvents(res, items)
			}
		} catch(err) {
			this.logger.error({ err }, 'error in sse subscription')
			if(res.writableEnded) {
				return
			}

			// send error event
			const message = err instanceof Error ? err.message : String(err)
			const errData	= jsonifier.stringify({ message })
			res.write(`event: error\ndata: ${errData}\nretry: 250\n\n`)
			res.end()
		}
	}

	function writeSseEvents(res: ServerResponse, items: IEvent<T>[]) {
		for(const { id, payload, topic } of items) {
			const data = jsonifier.stringify(payload)
			if(!maxReplayEvents) {
				// if replay is disabled, do not send an id field
				res.write(`event: ${topic}\ndata: ${data}\n\n`)
				continue
			}

			res.write(`id: ${id}\nevent: ${topic}\ndata: ${data}\n\n`)
		}
	}
}
