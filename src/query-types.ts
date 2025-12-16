export type QueryResult<T> = {
	rowCount: number
	rows: T[]
}

export interface PgClient {
	query<T = any>(query: string, params?: unknown[]): Promise<QueryResult<T>>
	exec?(query: string): Promise<unknown>
}

export interface PgReleasableClient extends PgClient {
	release: () => void
}

export interface PgPoolLike extends PgClient {
	connect: () => Promise<PgReleasableClient>
	on(ev: 'remove', handler: (cl: PgReleasableClient) => void): this
	off(ev: 'remove', handler: (cl: PgReleasableClient) => void): this
}

export type PgClientLike = PgClient | PgPoolLike
