import assert from 'node:assert'
import { createHash } from 'node:crypto'
import { createRetryHandler } from './retry-handler.ts'
import type { IEventHandler, IReadEvent, JSONifier, PgmbWebhookOpts, SerialisedEvent } from './types.ts'

/**
 * Create a handler that sends events to a webhook URL via HTTP POST.
 * @param url Where to send the webhook requests
 */
export function createWebhookHandler(
	{
		timeoutMs = 5_000,
		headers,
		retryOpts = {
			// retry after 5 minutes, then after 30 minutes
			retriesS: [5 * 60, 30 * 60]
		},
		jsonifier = JSON,
		serialiseEvent = createSimpleSerialiser(jsonifier)
	}: Partial<PgmbWebhookOpts>
) {
	const handler: IEventHandler = async(ev, { logger, extra }) => {
		assert(
			typeof extra === 'object'
			&& extra !== null
			&& 'url' in extra
			&& (
				typeof extra.url === 'string'
				|| extra.url instanceof URL
			),
			'webhook handler requires extra.url parameter'
		)
		const { url } = extra
		const { body, contentType } = serialiseEvent(ev)
		const { status, statusText, body: res } = await fetch(url, {
			method: 'POST',
			headers: {
				'content-type': contentType,
				'idempotency-key': getIdempotencyKeyHeader(ev),
				...headers
			},
			body,
			redirect: 'manual',
			signal: AbortSignal.timeout(timeoutMs)
		})
		// don't care about response body
		await res?.cancel().catch(() => { })
		// see: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Idempotency-Key
		if(status === 422) { // unprocessable request, do not retry
			logger.warn('webhook returned 422, dropping event')
			return
		}

		if(status < 200 || status >= 300) {
			throw new Error(`Non-2xx response: ${status} (${statusText})`)
		}
	}

	if(!retryOpts) {
		return handler
	}

	return createRetryHandler(retryOpts, handler)
}

function getIdempotencyKeyHeader(ev: IReadEvent) {
	const hasher = createHash('sha256')
	for(const item of ev.items) {
		hasher.update(item.id)
	}

	return hasher.digest('hex').slice(0, 16)
}

function createSimpleSerialiser(
	jsonifier: JSONifier
): ((ev: IReadEvent) => SerialisedEvent) {
	return ev => ({
		body: jsonifier.stringify({
			items: ev.items
				.map(({ id, payload, topic }) => ({ id, payload, topic }))
		}),
		contentType: 'application/json'
	})
}
