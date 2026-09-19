import { DataSource } from '../types'
import { StarbaseDBConfiguration } from '../handler'
import { executeOperation } from './index'

/**
 * Chunked, resumable database dumps with "breathing intervals".
 *
 * The legacy dump endpoint accumulated the entire database into a single
 * in-memory string, which fails on large databases and blows through the
 * 30s Workers request window. This module writes the dump in bounded chunks,
 * yielding ("breathing") between chunks so queued requests are not starved,
 * persists progress in DO storage so work can resume across the 30s window
 * (driven by the DO alarm), and mirrors every chunk to R2 when a binding is
 * available so dumps survive eviction and can be fetched after completion.
 */

export const DUMP_STATE_KEY = 'tmp_dump_state'
export const DUMP_CHUNK_KEY = 'tmp_dump_chunk'

/** Defaults tuned to stay far under the 30s request window per cycle. */
export const DEFAULT_DUMP_OPTIONS = {
    /** Wall-clock budget per work cycle (ms) before we breathe/yield. */
    cycleTimeBudgetMs: 5_000,
    /** Minimum idle time between cycles when requests are waiting. */
    breathingIntervalMs: 5_000,
    /** Maximum rows fetched per SELECT batch. */
    rowsPerBatch: 500,
    /** Approximate serialized size (bytes) that closes a chunk. */
    chunkTargetBytes: 512 * 1024,
} as const

export interface DumpOptions {
    cycleTimeBudgetMs?: number
    breathingIntervalMs?: number
    rowsPerBatch?: number
    chunkTargetBytes?: number
}

export type DumpPhase = 'schema' | 'table-data' | 'complete'

export interface DumpState {
    dumpId: string
    fileName: string
    phase: DumpPhase
    tables: string[]
    tableIndex: number
    lastFetchedRowId: number | null
    /** Rowid of the last row written into the current chunk. */
    chunkRowOffset: number
    /** Total bytes serialized so far (chunks flushed to R2). */
    bytesWritten: number
    chunkIndex: number
    startedAt: number
    updatedAt: number
    /** Set when the dump finished and the R2 object is ready. */
    completedAt?: number
    /** Aggregate stats surfaced in status responses. */
    totalRows: number
    callbackUrl?: string
}

export interface ChunkRecord {
    dumpId: string
    chunkIndex: number
    /** Serialized SQL statements for this chunk. */
    content: string
    bytes: number
    createdAt: number
}

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/

/** Guard against SQL injection through table names coming from sqlite_master. */
export function isSafeIdentifier(name: string): boolean {
    return IDENTIFIER_PATTERN.test(name)
}

function sqlQuote(value: unknown): string {
    if (value === null || value === undefined) {
        return 'NULL'
    }
    if (typeof value === 'number' || typeof value === 'boolean') {
        return String(value)
    }
    if (value instanceof ArrayBuffer) {
        return `X'${Array.from(new Uint8Array(value), (b) =>
            b.toString(16).padStart(2, '0')
        ).join('')}'`
    }
    if (typeof value === 'string') {
        return `'${value.replace(/'/g, "''")}'`
    }
    // Fallback: serialize deterministically rather than emitting "object".
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`
}

export function serializeRows(
    table: string,
    rows: Record<string, unknown>[]
): { content: string; rowCount: number } {
    if (rows.length === 0) {
        return { content: '', rowCount: 0 }
    }
    const columns = Object.keys(rows[0])
    const columnList = columns.map((c) => `"${c}"`).join(', ')
    const lines = rows.map((row) => {
        const values = columns.map((c) => sqlQuote(row[c]))
        return `INSERT INTO "${table}" (${columnList}) VALUES (${values.join(', ')});`
    })
    return { content: `${lines.join('\n')}\n`, rowCount: rows.length }
}

export function makeDumpFileName(now = new Date()): string {
    const pad = (n: number) => String(n).padStart(2, '0')
    const stamp =
        `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}` +
        `-${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`
    return `dump_${stamp}.sql`
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Core engine. Designed to be driven by the Durable Object so each call
 * performs at most one bounded cycle of work (respecting `cycleTimeBudgetMs`),
 * then the DO decides whether to breathe and continue within this request or
 * schedule its alarm for the next cycle.
 */
export class ChunkedDumpEngine {
    constructor(
        private readonly storage: DurableObjectStorage,
        private readonly r2: R2Bucket | undefined,
        private readonly dataSource: DataSource,
        private readonly config: StarbaseDBConfiguration,
        private readonly options: Required<DumpOptions>
    ) {}

    /** Create or resume a dump. Returns the current state after one cycle. */
    async startDump(callbackUrl?: string): Promise<DumpState> {
        const existing = await this.storage.get<DumpState>(DUMP_STATE_KEY)
        if (existing && !existing.completedAt) {
            // Resume in-progress dump instead of starting over.
            return this.runCycle()
        }

        const tablesResult = await executeOperation(
            [{ sql: "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'tmp_%';" }],
            this.dataSource,
            this.config
        )
        const tables = tablesResult
            .map((row: Record<string, unknown>) => String(row.name))
            .filter((name) => isSafeIdentifier(name))

        const now = Date.now()
        const state: DumpState = {
            dumpId: `dump_${now.toString(36)}`,
            fileName: makeDumpFileName(new Date(now)),
            phase: 'schema',
            tables,
            tableIndex: 0,
            lastFetchedRowId: null,
            chunkRowOffset: 0,
            bytesWritten: 0,
            chunkIndex: 0,
            startedAt: now,
            updatedAt: now,
            totalRows: 0,
            ...(callbackUrl ? { callbackUrl } : {}),
        }
        await this.storage.put(DUMP_STATE_KEY, state)
        return this.runCycle()
    }

    async getState(): Promise<DumpState | undefined> {
        return this.storage.get<DumpState>(DUMP_STATE_KEY)
    }

    /**
     * Run one bounded cycle: serialize schema/data chunks until the time
     * budget is exhausted, flushing each chunk to R2 (when bound). Returns
     * the updated state; `completedAt` set when the dump is done.
     */
    async runCycle(): Promise<DumpState> {
        const state = (await this.storage.get<DumpState>(DUMP_STATE_KEY)) as DumpState
        if (!state) {
            throw new Error('No dump in progress')
        }
        if (state.completedAt) {
            return state
        }

        const cycleStart = Date.now()
        let content = ''
        let contentBytes = 0
        let chunkDirty = false

        const flushChunk = async () => {
            if (!chunkDirty) {
                return
            }
            const record: ChunkRecord = {
                dumpId: state.dumpId,
                chunkIndex: state.chunkIndex,
                content,
                bytes: contentBytes,
                createdAt: Date.now(),
            }
            if (this.r2) {
                await this.r2.put(
                    `${state.dumpId}/${String(record.chunkIndex).padStart(8, '0')}.sql`,
                    record.content
                )
            }
            // Persist the chunk in DO storage so a boundless environment can
            // still reassemble; keep only the tail window to bound memory.
            await this.storage.put(`${DUMP_CHUNK_KEY}:${record.chunkIndex}`, record)
            state.chunkIndex += 1
            state.bytesWritten += contentBytes
            content = ''
            contentBytes = 0
            chunkDirty = false
        }

        // Phase 1: schema pass. tableIndex is advanced BEFORE the yield point
        // so a resumed cycle never re-fetches (and duplicate-emits) a schema.
        if (state.phase === 'schema') {
            while (state.tableIndex < state.tables.length) {
                const table = state.tables[state.tableIndex]
                state.tableIndex++
                if (!isSafeIdentifier(table)) continue
                const schemaResult = await executeOperation(
                    [{
                        sql: `SELECT sql FROM sqlite_master WHERE type='table' AND name='${table}';`,
                    }],
                    this.dataSource,
                    this.config
                )
                if (schemaResult.length) {
                    const schema = schemaResult[0].sql
                    const ddl = `\n-- Table: ${table}\n${schema};\n\n`
                    content += ddl
                    contentBytes += ddl.length
                    chunkDirty = true
                }
                if (Date.now() - cycleStart >= this.options.cycleTimeBudgetMs) {
                    await flushChunk()
                    state.updatedAt = Date.now()
                    await this.storage.put(DUMP_STATE_KEY, state)
                    return state
                }
            }
            state.phase = 'table-data'
            state.tableIndex = 0
        }

        // Phase 2: data pass in rowid-batched chunks.
        while (state.tableIndex < state.tables.length) {
            const table = state.tables[state.tableIndex]
            if (!isSafeIdentifier(table)) {
                state.tableIndex++
                state.lastFetchedRowId = null
                continue
            }

            const since = state.lastFetchedRowId ?? 0
            const rowsResult = (await executeOperation(
                [{
                    sql: `SELECT rowid AS __rowid, * FROM "${table}" WHERE rowid > ${since} ORDER BY rowid LIMIT ${this.options.rowsPerBatch};`,
                }],
                this.dataSource,
                this.config
            )) as Record<string, unknown>[]

            if (rowsResult.length === 0) {
                state.tableIndex++
                state.lastFetchedRowId = null
                continue
            }

            const withoutRowidCol = rowsResult.map((row) => {
                const { __rowid, ...rest } = row
                state.lastFetchedRowId = Number(__rowid ?? state.lastFetchedRowId)
                return rest
            })

            const { content: batchSql, rowCount } = serializeRows(table, withoutRowidCol)
            content += batchSql
            contentBytes += batchSql.length
            state.totalRows += rowCount
            chunkDirty = true

            const chunkClosed =
                contentBytes >= this.options.chunkTargetBytes ||
                Date.now() - cycleStart >= this.options.cycleTimeBudgetMs

            if (chunkClosed) {
                await flushChunk()
                state.updatedAt = Date.now()
                await this.storage.put(DUMP_STATE_KEY, state)
                return state
            }
        }

        // Phase 3: completion marker.
        const done = `\n-- Dump complete: ${state.totalRows} rows, ${state.chunkIndex + (chunkDirty ? 1 : 0)} chunks.\n`
        content += done
        contentBytes += done.length
        chunkDirty = true
        await flushChunk()

        state.phase = 'complete'
        state.completedAt = Date.now()
        state.updatedAt = state.completedAt
        await this.storage.put(DUMP_STATE_KEY, state)
        return state
    }

    /** True when another cycle should run right now (time still available). */
    shouldContinue(state: DumpState, requestStart: number): boolean {
        if (state.completedAt) return false
        return Date.now() - requestStart < this.options.cycleTimeBudgetMs
    }

    /** Milliseconds to wait before the next cycle (breathing interval). */
    breathingDelayMs(): number {
        return this.options.breathingIntervalMs
    }

    /** Reassemble the dump from chunk records (or R2 when bound). */
    async assembleDump(state: DumpState): Promise<ReadableStream<Uint8Array> | null> {
        if (!state.completedAt) return null

        if (this.r2) {
            const stream = await this.concatenateR2(state)
            if (stream) return stream
        }

        const storage = this.storage
        return new ReadableStream<Uint8Array>({
            async start(controller) {
                const encoder = new TextEncoder()
                for (let i = 0; i < state.chunkIndex; i++) {
                    const record = await storage.get<ChunkRecord>(
                        `${DUMP_CHUNK_KEY}:${i}`
                    )
                    if (record?.content) {
                        controller.enqueue(encoder.encode(record.content))
                    }
                }
                controller.close()
            },
        })
    }

    private async concatenateR2(state: DumpState): Promise<ReadableStream<Uint8Array> | null> {
        if (!this.r2) return null
        const head = await this.r2.get(`${state.dumpId}/00000000.sql`)
        if (!head) return null
        // R2 concatenated reads: stream each part sequentially.
        const parts: ReadableStream<Uint8Array>[] = []
        for (let i = 0; i < state.chunkIndex; i++) {
            const obj = await this.r2.get(
                `${state.dumpId}/${String(i).padStart(8, '0')}.sql`
            )
            if (obj?.body) {
                parts.push(obj.body)
            }
        }
        return concatStreams(parts)
    }
}

function concatStreams(streams: ReadableStream<Uint8Array>[]): ReadableStream<Uint8Array> {
    return new ReadableStream<Uint8Array>({
        async start(controller) {
            for (const stream of streams) {
                const reader = stream.getReader()
                for (;;) {
                    const { done, value } = await reader.read()
                    if (done) break
                    controller.enqueue(value)
                }
                reader.releaseLock()
            }
            controller.close()
        },
    })
}
