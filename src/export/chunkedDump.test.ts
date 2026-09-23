import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
    ChunkedDumpEngine,
    DEFAULT_DUMP_OPTIONS,
    isSafeIdentifier,
    quoteIdentifier,
    makeDumpFileName,
    serializeRows,
    DUMP_STATE_KEY,
    DUMP_CHUNK_KEY,
    type ChunkRecord,
    type DumpOptions,
    type DumpState,
} from './chunkedDump'
import { executeOperation } from './index'
import type { DataSource } from '../types'
import type { StarbaseDBConfiguration } from '../handler'

vi.mock('./index', () => ({
    executeOperation: vi.fn(),
}))

type StorageMap = Map<string, unknown>

const makeStorage = () => {
    const map: StorageMap = new Map()
    return {
        get: vi.fn(async <T>(key: string) => map.get(key) as T),
        put: vi.fn(async (key: string, value: unknown) => {
            map.set(key, value)
        }),
        delete: vi.fn(async (key: string) => {
            map.delete(key)
        }),
        _map: map,
    }
}

const makeR2 = () => ({
    put: vi.fn(async () => undefined),
    get: vi.fn(async () => null),
})

const makeDataSource = (): DataSource =>
    ({ source: 'internal', rpc: {} }) as unknown as DataSource

const makeConfig = (): StarbaseDBConfiguration => ({ role: 'admin' })

const makeEngine = (overrides: Partial<DumpOptions> = {}) => {
    const storage = makeStorage()
    const r2 = makeR2()
    const engine = new ChunkedDumpEngine(
        storage as unknown as DurableObjectStorage,
        r2 as unknown as R2Bucket,
        makeDataSource(),
        makeConfig(),
        { ...DEFAULT_DUMP_OPTIONS, ...overrides }
    )
    return { engine, storage, r2 }
}

beforeEach(() => {
    vi.clearAllMocks()
})

describe('identifier safety', () => {
    it('accepts ordinary table names', () => {
        expect(isSafeIdentifier('users')).toBe(true)
        expect(isSafeIdentifier('order_items2')).toBe(true)
    })

    it('rejects injection attempts', () => {
        expect(isSafeIdentifier('users; DROP TABLE users')).toBe(false)
        expect(isSafeIdentifier('users"')).toBe(false)
        expect(isSafeIdentifier('tmp_cache')).toBe(true) // syntactically safe
    })
})

describe('row serialization', () => {
    it('escapes single quotes in strings', () => {
        const { content } = serializeRows('users', [{ name: "O'Brien" }])
        expect(content).toContain("'O''Brien'")
        expect(content).toContain('INSERT INTO "users"')
    })

    it('renders NULLs, numbers and booleans unquoted', () => {
        const { content } = serializeRows('t', [{ a: null, b: 1.5, c: true }])
        expect(content).toContain('(NULL, 1.5, true)')
    })

    it('returns empty content for zero rows', () => {
        const { content, rowCount } = serializeRows('t', [])
        expect(content).toBe('')
        expect(rowCount).toBe(0)
    })

    it('quotes identifiers and serializes null and binary values', () => {
        const { content } = serializeRows('order "items"', [
            { 'value "x"': null, payload: new Uint8Array([0, 15, 255]) },
        ])
        expect(content).toContain('INSERT INTO "order ""items"""')
        expect(content).toContain('("value ""x""", "payload")')
        expect(content).toContain("(NULL, X'000fff')")
    })
})

describe('dump file naming', () => {
    it('follows dump_YYYYMMDD-HHMMSS.sql in UTC', () => {
        const name = makeDumpFileName(new Date('2024-01-01T17:00:00Z'))
        expect(name).toBe('dump_20240101-170000.sql')
    })
})

describe('ChunkedDumpEngine cycles', () => {
    const setupTables = (tables: string[], schemas: Record<string, string>) => {
        vi.mocked(executeOperation).mockImplementation(async (queries: any) => {
            const sql: string = queries[0].sql
            const params: unknown[] = queries[0].params ?? []
            if (sql.includes("type='table' AND name NOT LIKE")) {
                return tables.map((name) => ({ name }))
            }
            if (sql.includes('SELECT sql FROM sqlite_master')) {
                const name = String(params[0] ?? '')
                return schemas[name] ? [{ sql: schemas[name] }] : []
            }
            if (sql.includes('PRAGMA table_info')) {
                return [{ name: 'id', pk: 1 }]
            }
            if (sql.includes('LIMIT 0')) {
                return []
            }
            if (sql.includes('ORDER BY')) {
                const since = Number(params[0] ?? -1)
                if (since < 2) {
                    return since < 0
                        ? [
                              {
                                  __starbase_dump_cursor_0: 0,
                                  id: 0,
                              },
                              {
                                  __starbase_dump_cursor_0: 1,
                                  id: 1,
                              },
                          ]
                        : [
                              {
                                  __starbase_dump_cursor_0: since + 1,
                                  id: since + 1,
                              },
                          ]
                }
                return []
            }
            return []
        })
    }

    it('completes a small dump in one cycle and writes chunks to R2', async () => {
        setupTables(['users'], { users: 'CREATE TABLE users (id INTEGER)' })
        vi.mocked(executeOperation).mockImplementation(async (queries: any) => {
            const sql: string = queries[0].sql
            const params: unknown[] = queries[0].params ?? []
            if (sql.includes("name NOT LIKE 'tmp_%'")) {
                return [{ name: 'users' }]
            }
            if (sql.includes('SELECT sql FROM sqlite_master')) {
                return [{ sql: 'CREATE TABLE users (id INTEGER)' }]
            }
            if (sql.includes('PRAGMA table_info')) {
                return [{ name: 'id', pk: 1 }]
            }
            if (sql.includes('LIMIT 0')) {
                return []
            }
            if (sql.includes('ORDER BY')) {
                const since = Number(params[0] ?? -1)
                if (since < 0) {
                    return [
                        { __starbase_dump_cursor_0: -2, id: -2 },
                        { __starbase_dump_cursor_0: 0, id: 0 },
                    ]
                }
                return since < 1 ? [{ __starbase_dump_cursor_0: 1, id: 1 }] : []
            }
            return []
        })

        const { engine, storage, r2 } = makeEngine()
        const state = await engine.startDump()

        expect(state.completedAt).toBeDefined()
        expect(state.phase).toBe('complete')
        expect(state.totalRows).toBe(3)
        expect(state.chunkIndex).toBeGreaterThan(0)
        expect(r2.put).toHaveBeenCalled()
        // Progress persisted in DO storage for resumability.
        const persisted = (await storage.get(DUMP_STATE_KEY)) as
            | DumpState
            | undefined
        expect(persisted?.completedAt).toBeDefined()
    })

    it('yields mid-dump when the cycle time budget is exhausted, then resumes', async () => {
        setupTables(['users'], { users: 'CREATE TABLE users (id INTEGER)' })
        let call = 0
        vi.mocked(executeOperation).mockImplementation(async (queries: any) => {
            const sql: string = queries[0].sql
            const params: unknown[] = queries[0].params ?? []
            if (sql.includes("name NOT LIKE 'tmp_%'"))
                return [{ name: 'users' }]
            if (sql.includes('SELECT sql FROM sqlite_master')) {
                return [{ sql: 'CREATE TABLE users (id INTEGER)' }]
            }
            if (sql.includes('PRAGMA table_info')) {
                return [{ name: 'id', pk: 1 }]
            }
            if (sql.includes('LIMIT 0')) {
                return []
            }
            if (sql.includes('ORDER BY')) {
                const since = Number(params[0] ?? -1)
                call++
                return since < 3
                    ? [{ __starbase_dump_cursor_0: since + 1, id: since + 1 }]
                    : []
            }
            return []
        })

        // 0ms budget: every data batch closes the cycle → resumable steps.
        const { engine } = makeEngine({ cycleTimeBudgetMs: 0 })
        const first = await engine.startDump()
        expect(first.completedAt).toBeUndefined()
        expect(['schema', 'table-data']).toContain(first.phase)

        // Resume cycles until complete.
        let state = first
        for (let i = 0; i < 50 && !state.completedAt; i++) {
            state = await engine.runCycle()
        }
        expect(state.completedAt).toBeDefined()
        expect(state.totalRows).toBeGreaterThan(0)
    })

    it('quotes valid and unusual table names instead of dropping them', async () => {
        const dataSqls: string[] = []
        vi.mocked(executeOperation).mockImplementation(async (queries: any) => {
            const sql: string = queries[0].sql
            const params: unknown[] = queries[0].params ?? []
            if (sql.includes("name NOT LIKE 'tmp_%'")) {
                return [{ name: 'good_table' }, { name: 'order items "2026"' }]
            }
            if (sql.includes('SELECT sql FROM sqlite_master')) {
                return [
                    { sql: 'CREATE TABLE "order items ""2026""" (id INTEGER)' },
                ]
            }
            if (sql.includes('PRAGMA table_info')) {
                return [{ name: 'id', pk: 1 }]
            }
            if (sql.includes('LIMIT 0')) return []
            if (sql.includes('ORDER BY')) {
                dataSqls.push(sql)
                return Number(params[0] ?? -1) < 0
                    ? [{ __starbase_dump_cursor_0: 1, id: 1 }]
                    : []
            }
            return []
        })

        const { engine, storage } = makeEngine()
        const state = await engine.startDump()
        const persisted = (await storage.get(DUMP_STATE_KEY)) as
            | DumpState
            | undefined
        expect(persisted?.tables).toEqual(['good_table', 'order items "2026"'])
        expect(
            dataSqls.some((sql) =>
                sql.includes(quoteIdentifier('order items "2026"'))
            )
        ).toBe(true)
        expect(state.totalRows).toBe(2)
    })

    it('startDump resumes an in-progress dump instead of restarting', async () => {
        setupTables(['users'], {})
        const { engine, storage } = makeEngine({ cycleTimeBudgetMs: 0 })
        // Seed an in-progress state.
        const seed: DumpState = {
            dumpId: 'dump_seed',
            fileName: 'dump_seed.sql',
            phase: 'table-data',
            tables: ['users'],
            tableIndex: 0,
            lastFetchedRowId: 3,
            chunkRowOffset: 0,
            bytesWritten: 0,
            chunkIndex: 0,
            startedAt: Date.now() - 1000,
            updatedAt: Date.now() - 500,
            totalRows: 0,
        }
        await storage.put(DUMP_STATE_KEY, seed)

        let dataSql = ''
        let dataParams: unknown[] = []
        vi.mocked(executeOperation).mockImplementation(async (queries: any) => {
            const sql: string = queries[0].sql
            if (sql.includes('ORDER BY')) {
                dataSql = sql
                dataParams = queries[0].params ?? []
                return []
            }
            return []
        })

        const state = await engine.startDump()
        expect(state.dumpId).toBe('dump_seed')
        expect(state.tables).toEqual(['users'])
        expect(dataSql).toContain('WHERE "rowid" > ?')
        expect(dataParams).toEqual([3])
    })

    it('assembleDump concatenates persisted chunks in order', async () => {
        const { engine, storage } = makeEngine()
        const state: DumpState = {
            dumpId: 'dump_x',
            fileName: 'dump_x.sql',
            phase: 'complete',
            tables: ['t'],
            tableIndex: 1,
            lastFetchedRowId: 3,
            chunkRowOffset: 0,
            bytesWritten: 10,
            chunkIndex: 2,
            startedAt: 1,
            updatedAt: 2,
            completedAt: 3,
            totalRows: 3,
        }
        const c0: ChunkRecord = {
            dumpId: 'dump_x',
            chunkIndex: 0,
            content: 'CREATE TABLE t (id INTEGER);\n',
            bytes: 30,
            createdAt: 1,
        }
        const c1: ChunkRecord = {
            dumpId: 'dump_x',
            chunkIndex: 1,
            content: 'INSERT INTO "t" ("id") VALUES (1);\n',
            bytes: 35,
            createdAt: 2,
        }
        await storage.put(`${DUMP_CHUNK_KEY}:0`, c0)
        await storage.put(`${DUMP_CHUNK_KEY}:1`, c1)

        const stream = await engine.assembleDump(state)
        expect(stream).not.toBeNull()
        const text = await new Response(stream as ReadableStream).text()
        expect(text).toContain('CREATE TABLE t')
        expect(text).toContain('VALUES (1)')
    })

    it('respects custom rowsPerBatch from options', async () => {
        let captured = ''
        vi.mocked(executeOperation).mockImplementation(async (queries: any) => {
            const sql: string = queries[0].sql
            if (sql.includes('ORDER BY')) {
                captured = sql
                return []
            }
            if (sql.includes("name NOT LIKE 'tmp_%'"))
                return [{ name: 'users' }]
            if (sql.includes('SELECT sql FROM sqlite_master')) {
                return [{ sql: 'CREATE TABLE users (id INTEGER)' }]
            }
            return []
        })
        const { engine } = makeEngine({ rowsPerBatch: 42 })
        await engine.startDump()
        expect(captured).toContain('LIMIT 42')
    })
})

// ---------------------------------------------------------------------------
// R2 multipart finalize + presigned URL
// ---------------------------------------------------------------------------

type UploadedPart = { partNumber: number; etag: string }

const makeMultipartR2 = () => {
    const uploaded: { partNumber: number; size: number }[] = []
    let completed: UploadedPart[] | null = null
    let resumedWith: string | null = null
    let createdFor: string | null = null
    let signedUrlResult: { url?: string } | null | undefined = undefined
    const deletedKeys: string[] = []
    const objects = new Map<string, { body: ReadableStream<Uint8Array> }>()

    const mpu = {
        key: '',
        uploadId: 'mpu-1',
        uploadPart: async (partNumber: number, value: Uint8Array) => {
            uploaded.push({ partNumber, size: value.length })
            return { partNumber, etag: `etag-${partNumber}` }
        },
        abort: async () => undefined,
        complete: async (parts: UploadedPart[]) => {
            completed = parts
            const total = uploaded.reduce((sum, p) => sum + p.size, 0)
            return { key: mpu.key, size: total }
        },
    }

    const r2 = {
        createMultipartUpload: async (key: string) => {
            createdFor = key
            mpu.key = key
            return mpu
        },
        resumeMultipartUpload: (key: string, uploadId: string) => {
            resumedWith = uploadId
            mpu.key = key
            return mpu
        },
        put: async () => undefined,
        get: async (key: string) => objects.get(key) ?? null,
        delete: async (key: string) => {
            deletedKeys.push(key)
        },
    }
    return {
        r2,
        uploaded,
        deleted: () => deletedKeys,
        completed: () => completed,
        resumedWith: () => resumedWith,
        createdFor: () => createdFor,
        objects,
        signedUrlResult: () => signedUrlResult,
        setSigned: (v: { url?: string } | null | undefined) => {
            signedUrlResult = v
        },
    }
}

const seedCompletedState = async (
    storage: ReturnType<typeof makeStorage>,
    chunks: string[],
    extra: Partial<DumpState> = {}
) => {
    const state: DumpState = {
        dumpId: 'dump_fin',
        fileName: 'dump_fin.sql',
        phase: 'complete',
        tables: ['t'],
        tableIndex: 1,
        lastFetchedRowId: null,
        chunkRowOffset: 0,
        bytesWritten: chunks.join('').length,
        chunkIndex: chunks.length,
        startedAt: 1,
        updatedAt: 2,
        completedAt: 3,
        totalRows: 0,
        ...extra,
    }
    for (let i = 0; i < chunks.length; i++) {
        const record: ChunkRecord = {
            dumpId: 'dump_fin',
            chunkIndex: i,
            content: chunks[i],
            bytes: chunks[i].length,
            createdAt: i,
        }
        await storage.put(`${DUMP_CHUNK_KEY}:${i}`, record)
    }
    await storage.put(DUMP_STATE_KEY, state)
    return state
}

describe('finalizeDump (R2 multipart upload + presigned URL)', () => {
    it('uploads part-sized parts, completes the object and cleans chunk mirrors', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        // 4 chunks of 10 bytes each; partSize 20 → parts of 20, 20, 20(tail).
        await seedCompletedState(storage, [
            'AAAAAAAAAA',
            'BBBBBBBBBB',
            'CCCCCCCCCC',
            'DDDDDDDDDD',
        ])
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        const { done, state } = await engine.finalizeDump({ partSizeBytes: 20 })

        expect(done).toBe(true)
        expect(m.createdFor()).toBe('dumps/dump_fin/dump_fin.sql')
        // 40 bytes at partSize 20 → exactly two full parts, no tail.
        expect(m.uploaded.map((p) => p.size)).toEqual([20, 20])
        expect(m.uploaded.map((p) => p.partNumber)).toEqual([1, 2])
        expect(m.completed()?.length).toBe(2)
        expect(state.finalObjectKey).toBe('dumps/dump_fin/dump_fin.sql')
        expect(state.finalObjectSize).toBe(40)
        expect(state.finalizedAt).toBeDefined()
        expect(state.temporaryChunksCleanedAt).toBeDefined()
        expect(m.deleted().length).toBe(4)
        expect(await storage.get(`${DUMP_CHUNK_KEY}:0`)).toBeUndefined()
    })

    it('retries temporary chunk cleanup after a finalized upload', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        await seedCompletedState(storage, ['data'])
        const originalDelete = m.r2.delete
        let attempts = 0
        m.r2.delete = vi.fn(async (key: string) => {
            attempts++
            if (attempts === 1) throw new Error('temporary delete failure')
            return originalDelete(key)
        })
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            DEFAULT_DUMP_OPTIONS
        )

        const first = await engine.finalizeDump()
        expect(first.done).toBe(false)
        expect(first.state.temporaryChunksCleanedAt).toBeUndefined()
        expect(await storage.get(`${DUMP_CHUNK_KEY}:0`)).toBeDefined()
        const second = await engine.finalizeDump()
        expect(second.done).toBe(true)
        expect(second.state.temporaryChunksCleanedAt).toBeDefined()
    })

    it('resumes an interrupted finalize without re-uploading completed parts', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        const chunks = ['AAAAAAAAAA', 'BBBBBBBBBB', 'CCCCCCCCCC', 'DDDDDDDDDD']
        // First part (chunk 0 + chunk 1 = 20 bytes) already uploaded.
        await seedCompletedState(storage, chunks, {
            finalObjectKey: 'dumps/dump_fin/dump_fin.sql',
            finalizeUploadId: 'mpu-9',
            finalizeParts: [{ partNumber: 1, etag: 'etag-1' }],
            finalizeBytes: 20,
        })
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        const { done } = await engine.finalizeDump({ partSizeBytes: 20 })

        expect(done).toBe(true)
        expect(m.resumedWith()).toBe('mpu-9')
        // Only the remaining 20 bytes upload, as part 2 — no re-upload.
        expect(m.uploaded).toEqual([{ partNumber: 2, size: 20 }])
        expect(m.completed()?.length).toBe(2)
    })

    it('returns done:false when the time budget runs out mid-upload, then finishes on the next cycle', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        await seedCompletedState(storage, [
            'AAAAAAAAAA',
            'BBBBBBBBBB',
            'CCCCCCCCCC',
            'DDDDDDDDDD',
        ])
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        const first = await engine.finalizeDump({
            partSizeBytes: 20,
            timeBudgetMs: -1,
        })
        expect(first.done).toBe(false)
        expect(first.state.finalizeUploadId).toBe('mpu-1')

        const second = await engine.finalizeDump({ partSizeBytes: 20 })
        expect(second.done).toBe(true)
        expect(second.state.finalizedAt).toBeDefined()
    })

    it('finalizes without R2 binding (streaming-only environments)', async () => {
        const storage = makeStorage()
        await seedCompletedState(storage, ['data'])
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            undefined as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        const { done, state } = await engine.finalizeDump()
        expect(done).toBe(true)
        expect(state.finalObjectKey).toBeUndefined()
        expect(state.finalizedAt).toBeDefined()
    })

    it('getPresignedUrl returns the signed URL when the runtime supports createSignedUrl', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        await seedCompletedState(storage, ['data'], {
            finalObjectKey: 'dumps/dump_fin/dump_fin.sql',
            finalizedAt: 9,
        })
        m.setSigned({ url: 'https://signed.example/dump?sig=abc' })
        const r2 = Object.assign(m.r2, {
            createSignedUrl: async () =>
                m.signedUrlResult() as { url?: string },
        })
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        const url = await engine.getPresignedUrl(600)
        expect(url).toBe('https://signed.example/dump?sig=abc')
    })

    it('getPresignedUrl returns null when the binding lacks createSignedUrl or the URL shape is unexpected', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        await seedCompletedState(storage, ['data'], {
            finalObjectKey: 'dumps/dump_fin/dump_fin.sql',
            finalizedAt: 9,
        })
        // Older binding: no createSignedUrl at all.
        const engineA = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        await expect(engineA.getPresignedUrl()).resolves.toBeNull()

        // Unexpected result shape (missing url) must not throw.
        m.setSigned({})
        const r2 = Object.assign(m.r2, {
            createSignedUrl: async () =>
                m.signedUrlResult() as { url?: string },
        })
        const engineB = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        await expect(engineB.getPresignedUrl()).resolves.toBeNull()
    })

    it('does not return an empty fallback after temporary chunks are cleaned', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        const state = await seedCompletedState(storage, ['chunk-content'], {
            finalObjectKey: 'dumps/dump_fin/dump_fin.sql',
            finalizedAt: 9,
            temporaryChunksCleanedAt: 10,
        })
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            DEFAULT_DUMP_OPTIONS
        )

        await expect(engine.assembleDump(state)).resolves.toBeNull()
    })

    it('assembleDump prefers the consolidated final object', async () => {
        const storage = makeStorage()
        const m = makeMultipartR2()
        await seedCompletedState(storage, ['chunk-content'], {
            finalObjectKey: 'dumps/dump_fin/dump_fin.sql',
            finalizedAt: 9,
        })
        const bytes = new TextEncoder().encode('CONSOLIDATED')
        m.objects.set('dumps/dump_fin/dump_fin.sql', {
            body: new ReadableStream<Uint8Array>({
                start(c) {
                    c.enqueue(bytes)
                    c.close()
                },
            }),
        })
        const engine = new ChunkedDumpEngine(
            storage as unknown as DurableObjectStorage,
            m.r2 as unknown as R2Bucket,
            makeDataSource(),
            makeConfig(),
            { ...DEFAULT_DUMP_OPTIONS }
        )
        const state = (await storage.get(DUMP_STATE_KEY)) as DumpState
        const stream = await engine.assembleDump(state)
        const text = await new Response(stream as ReadableStream).text()
        expect(text).toBe('CONSOLIDATED')
    })
})
