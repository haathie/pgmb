import { RETRY_EVENT } from './consts.ts'
import { findEvents, scheduleEventRetry } from './queries.ts'
import type { IEvent, IEventData, IEventHandler, IRetryHandlerOpts, RetryEventPayload } from './types.ts'

type IMaybeRetryEvent<T extends IEventData> = {
	items: IEvent<T>[]
	retryPayload?: RetryEventPayload
}

export function createRetryHandler<T extends IEventData>(
	{ retriesS }: IRetryHandlerOpts,
	handler: IEventHandler<T>,
): IEventHandler<T> {
	return async(ev, ctx) => {
		const { client, subscriptionId } = ctx
		const evs: IMaybeRetryEvent<T>[] = []
		const idsToLoad: string[] = []

		const items = [...ev.items]

		for(let i = 0; i < items.length;) {
			const { topic, payload: _p } = items[i]
			if(topic !== RETRY_EVENT) {
				i++
				continue
			}

			const retryPayload = _p as RetryEventPayload
			if(retryPayload.ids?.length) {
				evs.push({ items: [], retryPayload })
				idsToLoad.push(...retryPayload.ids)
			}

			items.splice(i, 1)
		}

		if(items.length) {
			evs.push({ items })
		}

		if(idsToLoad) {
			const fetchedEvents = await findEvents.run({ ids: idsToLoad }, client)
			const fetchedEventMap = fetchedEvents.reduce(
				(map, ev) => {
					map[ev.id] = ev as IEvent<T>
					return map
				},
				{} as { [id: string]: IEvent<T> }
			)

			ctx.logger.debug(
				{
					idsToLoad: idsToLoad.length,
					fetchedCount: fetchedEvents.length
				},
				'loaded events for retry'
			)

			// populate the events
			for(const { items, retryPayload } of evs) {
				if(!retryPayload) {
					continue
				}

				for(const id of retryPayload.ids) {
					const ev = fetchedEventMap[id]
					if(!ev) {
						ctx.logger.warn({ id }, 'event to retry not found')
						continue
					}

					items.push(ev)
				}
			}
		}

		for(const ev of evs) {
			const logger = ctx.logger.child({
				retryNumber: ev.retryPayload?.retryNumber,
				ids: ev.items.map(i => i.id)
			})

			try {
				await handler(ev,	{ ...ctx, logger })
			} catch(err) {
				const retryNumber = (ev.retryPayload?.retryNumber ?? 0)
				const nextRetryGapS = retriesS[retryNumber]
				logger.error({ err, nextRetryGapS }, 'error in event handler')

				if(!nextRetryGapS) {
					return
				}

				await scheduleEventRetry.run(
					{
						subscriptionId,
						ids: ev.items.map(i => i.id),
						retryNumber: retryNumber + 1,
						delayInterval: `${nextRetryGapS} seconds`
					},
					client
				)
			}
		}
	}
}
