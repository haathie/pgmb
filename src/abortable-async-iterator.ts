import assert from 'assert'

type AAResult<T> = IteratorResult<T>

export class AbortableAsyncIterator<T> implements AsyncIterableIterator<T> {
	readonly signal: AbortSignal
	readonly onEnd: () => void

	ended = false

	#resolve: (() => void) | undefined
	#reject: ((reason?: unknown) => void) | undefined
	#queue: T[] = []
	#locked = false

	constructor(signal: AbortSignal, onEnd: () => void = () => {}) {
		this.signal = signal
		this.onEnd = onEnd
		signal.addEventListener('abort', this.#onAbort)
	}

	async next(): Promise<AAResult<T>> {
		assert(!this.ended, 'Iterator has already been completed')
		assert(!this.#locked, 'Concurrent calls to next() are not allowed')

		let nextItem = this.#queue.shift()
		if(nextItem) {
			return { value: nextItem, done: false }
		}

		this.#locked = true
		try {
			await this.#setupNextPromise()
		} finally {
			this.#locked = false
		}

		nextItem = this.#queue.shift()
		if(nextItem) {
			return { value: nextItem, done: false }
		}

		return { value: undefined, done: true }
	}

	enqueue(value: T) {
		assert(!this.ended, 'Iterator has already been completed')
		this.#queue.push(value)
		this.#resolve?.()
	}

	throw(reason?: unknown): Promise<AAResult<T>> {
		this.signal.throwIfAborted()
		this.#reject?.(reason)
		this.#end()
		return Promise.resolve({ done: true, value: undefined })
	}

	return(value?: any): Promise<AAResult<T>> {
		this.#resolve?.()
		this.#end()
		return Promise.resolve({ done: true, value })
	}

	#setupNextPromise() {
		return new Promise<void>((resolve, reject) => {
			this.#resolve = () => {
				resolve()
				this.#cleanupTask()
			}

			this.#reject = err => {
				reject(err)
				this.#cleanupTask()
			}
		})
	}

	#cleanupTask() {
		this.#resolve = undefined
		this.#reject = undefined
	}

	#onAbort = (reason: any) => {
		this.#reject?.(reason)
		this.#end()
		this.ended = true
	}

	#end() {
		this.signal.removeEventListener('abort', this.#onAbort)
		this.onEnd()
	}

	[Symbol.asyncIterator]() {
		return this
	}
}
