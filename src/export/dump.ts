import { executeOperation } from './index'
import { StarbaseDBConfiguration } from '../handler'
import { DataSource } from '../types'
import { createResponse } from '../utils'
import {
    ChunkedDumpEngine,
    DEFAULT_DUMP_OPTIONS,
    type DumpOptions,
    type DumpState,
} from './chunkedDump'

/**
 * Dump route.
 *
 * Small databases keep the legacy behavior: the dump is fully serialized and
 * returned inline as a downloadable file (well under the 30s window).
 *
 * Large databases exceed the 30s Workers request window, so `?job=1` opts
 * into a resumable job flow driven inside the Durable Object: bounded work
 * cycles with breathing intervals, progress persisted in DO storage, chunks
 * mirrored to R2 when the binding exists, the DO alarm resuming work across
 * window boundaries, and an optional `callbackUrl` invoked on completion.
 */

export interface DumpJobEnv {
    /** Optional R2 binding; when absent, chunks persist in DO storage only. */
    R2_DUMP_BUCKET?: R2Bucket
}

export interface DumpEngineHost {
    storage: DurableObjectStorage
    env: DumpJobEnv
    dataSource: DataSource
    config: StarbaseDBConfiguration
    setAlarm: (time: number, options?: DurableObjectSetAlarmOptions) => Promise<void>
}

/** Query param parsing: `cycleMs`, `breathMs`, `rows`, `chunkBytes`. */
export function parseDumpOptions(searchParams: URLSearchParams): DumpOptions {
    const options: DumpOptions = {}
    const readNumber = (key: string, target: keyof DumpOptions) => {
        const raw = searchParams.get(key)
        if (raw === null) return
        const value = Number(raw)
        if (Number.isFinite(value) && value > 0) {
            options[target] = value as never
        }
    }
    readNumber('cycleMs', 'cycleTimeBudgetMs')
    readNumber('breathMs', 'breathingIntervalMs')
    readNumber('rows', 'rowsPerBatch')
    readNumber('chunkBytes', 'chunkTargetBytes')
    readNumber('partBytes', 'finalizePartSizeBytes')
    readNumber('finalizeMs', 'finalizeTimeBudgetMs')
    return options
}

/** Legacy inline dump path, behavior-identical for small databases. */
export async function legacyDump(
    dataSource: DataSource,
    config: StarbaseDBConfiguration
): Promise<Response> {
    try {
        const tablesResult = await executeOperation(
            [{ sql: "SELECT name FROM sqlite_master WHERE type='table';" }],
            dataSource,
            config
        )

        const tables = tablesResult.map((row: any) => row.name)
        let dumpContent = 'SQLite format 3\0' // SQLite file header

        for (const table of tables) {
            const schemaResult = await executeOperation(
                [{
                    sql: `SELECT sql FROM sqlite_master WHERE type='table' AND name='${table}';`,
                }],
                dataSource,
                config
            )

            if (schemaResult.length) {
                const schema = schemaResult[0].sql
                dumpContent += `\n-- Table: ${table}\n${schema};\n\n`
            }

            const dataResult = await executeOperation(
                [{ sql: `SELECT * FROM ${table};` }],
                dataSource,
                config
            )

            for (const row of dataResult) {
                const values = Object.values(row).map((value) =>
                    typeof value === 'string'
                        ? `'${value.replace(/'/g, "''")}'`
                        : value
                )
                dumpContent += `INSERT INTO ${table} VALUES (${values.join(', ')});\n`
            }

            dumpContent += '\n'
        }

        const blob = new Blob([dumpContent], { type: 'application/x-sqlite3' })

        const headers = new Headers({
            'Content-Type': 'application/x-sqlite3',
            'Content-Disposition': 'attachment; filename="database_dump.sql"',
        })

        return new Response(blob, { headers })
    } catch (error: any) {
        console.error('Database Dump Error:', error)
        return createResponse(undefined, 'Failed to create database dump', 500)
    }
}

/**
 * Chunked job dump, executed inside the Durable Object. Runs bounded cycles
 * within the current request, schedules the alarm for the next breathing
 * interval when work remains, and returns either the finished dump stream or
 * a 202 progress payload.
 */
export async function runDumpJob(
    host: DumpEngineHost,
    searchParams: URLSearchParams,
    requestStart: number = Date.now()
): Promise<Response> {
    try {
        const options = parseDumpOptions(searchParams)
        const engine = new ChunkedDumpEngine(
            host.storage,
            host.env.R2_DUMP_BUCKET,
            host.dataSource,
            host.config,
            { ...DEFAULT_DUMP_OPTIONS, ...options }
        )

        let state = await engine.getState()
        if (!state || state.completedAt) {
            state = await engine.startDump(
                searchParams.get('callbackUrl') ?? undefined
            )
        } else {
            state = await engine.runCycle()
        }

        // Keep working while this request still has budget (5s default cycle
        // budget bounds each burst; breathing happens between bursts).
        while (
            !state.completedAt &&
            Date.now() - requestStart < DEFAULT_DUMP_OPTIONS.cycleTimeBudgetMs
        ) {
            state = await engine.runCycle()
        }

        if (state.completedAt) {
            // Kick off the R2 multipart consolidation (presigned-URL-ready
            // single object) asynchronously via the DO alarm; the response
            // below streams from the chunk records either way.
            if (host.env.R2_DUMP_BUCKET && !state.finalizedAt) {
                try {
                    await host.setAlarm(Date.now() + 1_000)
                } catch (alarmError) {
                    console.error('Failed to schedule dump finalize:', alarmError)
                }
            }
            const stream = await engine.assembleDump(state)
            if (stream) {
                return new Response(stream, {
                    headers: {
                        'Content-Type': 'application/x-sqlite3',
                        'Content-Disposition': `attachment; filename="${state.fileName}"`,
                    },
                })
            }
        }

        // Work remains: breathe, then let the DO alarm drive the next cycle.
        const resumeAt = Date.now() + DEFAULT_DUMP_OPTIONS.breathingIntervalMs
        await host.setAlarm(resumeAt)

        return createResponse(
            {
                dumpId: state.dumpId,
                status: 'in-progress',
                phase: state.phase,
                progress: {
                    tablesTotal: state.tables.length,
                    tableIndex: state.tableIndex,
                    totalRows: state.totalRows,
                    bytesWritten: state.bytesWritten,
                    chunkIndex: state.chunkIndex,
                },
                resumeAt,
                fileName: state.fileName,
            },
            undefined,
            202
        )
    } catch (error: any) {
        console.error('Database Dump Error:', error)
        return createResponse(undefined, 'Failed to create database dump', 500)
    }
}

/**
 * Status + fetch endpoint for in-progress/completed chunked dumps.
 * `GET /export/dump?job=1` while a job runs returns 202 progress; once the
 * job is complete the assembled dump streams back.
 */
export async function dumpJobStatus(
    host: DumpEngineHost
): Promise<Response> {
    const engine = new ChunkedDumpEngine(
        host.storage,
        host.env.R2_DUMP_BUCKET,
        host.dataSource,
        host.config,
        DEFAULT_DUMP_OPTIONS
    )
    const state = await engine.getState()
    if (!state) {
        return createResponse(undefined, 'No dump job found', 404)
    }
    if (!state.completedAt) {
        return createResponse(
            {
                dumpId: state.dumpId,
                status: 'in-progress',
                phase: state.phase,
                progress: {
                    tablesTotal: state.tables.length,
                    tableIndex: state.tableIndex,
                    totalRows: state.totalRows,
                    bytesWritten: state.bytesWritten,
                    chunkIndex: state.chunkIndex,
                },
                fileName: state.fileName,
            },
            undefined,
            202
        )
    }
    // Completed + consolidated: prefer a presigned, expiring download URL
    // when the runtime supports it; otherwise fall back to streaming.
    if (state.finalObjectKey) {
        const downloadUrl = await engine.getPresignedUrl()
        if (downloadUrl) {
            return createResponse(
                {
                    dumpId: state.dumpId,
                    status: 'complete',
                    downloadUrl,
                    downloadUrlExpiresInSeconds: 3600,
                    downloadType: 'presigned-url',
                    finalObjectKey: state.finalObjectKey,
                    size: state.finalObjectSize ?? state.bytesWritten,
                    totalRows: state.totalRows,
                    fileName: state.fileName,
                },
                undefined,
                200
            )
        }
    }
    const stream = await engine.assembleDump(state)
    if (!stream) {
        return createResponse(undefined, 'Dump data unavailable', 410)
    }
    return new Response(stream, {
        headers: {
            'Content-Type': 'application/x-sqlite3',
            'Content-Disposition': `attachment; filename="${state.fileName}"`,
        },
    })
}

/**
 * One bounded finalize cycle for the completed dump: consolidates chunks
 * into the single R2 object via multipart upload. Driven by the DO alarm;
 * reschedules itself while parts remain (`{ done: false }`).
 */
export async function runDumpFinalize(host: DumpEngineHost): Promise<void> {
    const engine = new ChunkedDumpEngine(
        host.storage,
        host.env.R2_DUMP_BUCKET,
        host.dataSource,
        host.config,
        DEFAULT_DUMP_OPTIONS
    )
    const { done } = await engine.finalizeDump()
    if (!done) {
        await host.setAlarm(Date.now() + 1_000)
    }
}

export async function dumpDatabaseRoute(
    dataSource: DataSource,
    config: StarbaseDBConfiguration
): Promise<Response> {
    return legacyDump(dataSource, config)
}
