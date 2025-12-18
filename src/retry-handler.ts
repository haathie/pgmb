import { RETRY_EVENT } from './consts.ts'
import type { IReadNextEventsResult } from './queries.ts'
import { findEvents, scheduleEventRetry } from './queries.ts'
import type { PgClientLike } from './query-types.ts'
import type { IEvent, IEventData, IEventHandler, IReadEvent, IRetryEventPayload, IRetryHandlerOpts } from './types.ts'

export function createRetryHandler<T extends IEventData>(
	{ name, retriesS }: IRetryHandlerOpts,
	handler: IEventHandler<T>,
): IEventHandler<T> {
	return async(ev, ctx) => {
		const { client, subscriptionId, logger } = ctx

		try {
			await handler(ev,	ctx)
		} catch(err) {
			const retryNumber = (ev.retry?.retryNumber ?? 0)
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
					delayInterval: `${nextRetryGapS} seconds`,
					handlerName: name,
				},
				client
			)
		}
	}
}

export async function normaliseRetryEventsInReadEventMap<T extends IEventData>(
	rows: IReadNextEventsResult[],
	client: PgClientLike
) {
	const map: { [sid: string]: IReadEvent<T>[] } = {}
	const evsToPopulate: IReadEvent<T>[] = []
	const idsToLoad: string[] = []

	// reverse the map, do subscriptionId -> events
	const subToEventMap: { [sid: string]: IReadNextEventsResult[] } = {}
	for(const row of rows) {
		for(const subId of row.subscriptionIds) {
			subToEventMap[subId] ||= []
			subToEventMap[subId].push(row)
		}
	}

	const subEventList = Object.entries(subToEventMap)
	for(const [subscriptionId, items] of subEventList) {
		for(let i = 0;i < items.length;i) {
			const item = items[i]
			if(item.topic !== RETRY_EVENT) {
				i++
				continue
			}

			const retry = item.payload as IRetryEventPayload
			if(!retry.ids?.length) {
				continue
			}

			idsToLoad.push(...retry.ids)

			map[subscriptionId] ||= []

			const ev: IReadEvent<T> = { items: [], retry }
			map[subscriptionId].push(ev)
			evsToPopulate.push(ev)
			items.splice(i, 1)
		}

		if(!items.length) {
			continue
		}

		map[subscriptionId] ||= []
		map[subscriptionId].push({ items: items as unknown as IEvent<T>[] })
	}

	if(!idsToLoad.length) {
		return { map, retryEvents: 0, retryItemCount: 0 }
	}

	const fetchedEvents = await findEvents.run({ ids: idsToLoad }, client)
	const fetchedEventMap = fetchedEvents.reduce(
		(map, ev) => {
			map[ev.id] = ev as IEvent<T>
			return map
		},
		{} as { [id: string]: IEvent<T> }
	)

	// populate the events
	for(const { items, retry } of evsToPopulate) {
		if(!retry) {
			continue
		}

		for(const id of retry.ids) {
			const ev = fetchedEventMap[id]
			if(!ev) {
				continue
			}

			items.push(ev)
		}
	}

	return {
		map,
		retryEvents: evsToPopulate.length,
		retryItemCount: idsToLoad.length,
	}
}
