// Copyright _!_
//
// License _!_

import { promisify } from 'util'

import dayjs from 'dayjs'
import mapKeys from 'lodash.mapkeys'
import camelCase from 'lodash.camelcase'
import pg from 'pg'
import Cursor from 'pg-cursor'
// USE THIS LIBRARY TO WRITE SQL QUERIES.
// IT PREVENTS SQL INJECTION BY PARAMETERIZING QUERIES.
import sql from 'sql-template-strings'

export const ALL_FIL_ADDRESS = 'all'
const NIL_UUID = '00000000-0000-0000-0000-000000000000'

const cl = console.log

const { Pool, types } = pg

Cursor.prototype.readAsync = promisify(Cursor.prototype.read)

types.setTypeParser(20, val => BigInt(val)) // pg bigint -> JS Number
types.setTypeParser(1700, val => Number(val)) // pg numeric -> JS Number
types.setTypeParser(1114, str => dayjs.utc(str).toDate()) // pg date -> JS Date

export let pool
export let readPool
const nodeIpToId = {}

export function init (opts = {}) {
    pool = createPool(opts)
    readPool = createPool({
        ...opts,
        host: process.env.PG_READER_HOST ?? process.env.PGHOST,
    })
}

function createPool (opts = {}) {
    // Undocumented way to set runtime parameters for a Pool
    // https://github.com/brianc/node-postgres/issues/983#issuecomment-736075608
    //
    // https://www.postgresql.org/docs/12/wal-async-commit.html
    // TL;DR Significantly improves throughput for small transactions
    // at the cost of recent data loss if DB crashes.
    let pgOptions = ''
    if (opts.asyncCommit) {
        pgOptions = '-c synchronous_commit=off'
    }

    const pool = new Pool({
        options: pgOptions,
        application_name: opts.appName,
        ...opts,
    })
    pool.on('error', err => {
        cl('Postgres err', err)
        //errors.report(err)
    })
    pool.once('connect', () => {
        cl('Connected to Postgres')
    })

    return pool
}

function camelCaseRows (rows) {
    return rows.map(row => mapKeys(row, (_, key) => camelCase(key)))
}

export async function getCabooseRequests (sinceMinutes) {
    const query = `
        SELECT *
        FROM bandwidth_logs
        WHERE
            start_time >= now() - interval '${sinceMinutes} min'
            and node_id = 'a6a3592c-7f1a-41f5-9293-11d6b5647e5e'
            and user_agent = 'bifrost-staging-ny/'
            and log_sender = 'bifrost-gateway'
            order by start_time asc
        `

    const res = await readPool.query(query)
    const results = camelCaseRows(res.rows)
    return results
}
