
import fs from 'node:fs'
import fsp from 'node:fs/promises'
import readline from 'node:readline'
import { setTimeout } from 'node:timers/promises'

import minimist from 'minimist'
import percentile from 'percentile'
import lodash from 'lodash-es'

import * as db from './db.js'
import { sendRequestHttp1, sendRequestHttp2 } from './http.js'

BigInt.prototype.toJSON = function() { return Number(this) }
global.cl = console.log

const LOG_FILE = 'logs/logs.ndjson'

async function getLogs (sinceMinutes) {
    db.init()

    const res = await db.getCabooseRequests(sinceMinutes)
    const wstream = fs.createWriteStream(LOG_FILE)
    for (const row of res) {
        wstream.write(JSON.stringify(row) + '\n')
    }
    wstream.end()
}

// Get logs with past timestamps modified into future timestamps
async function getModifiedLogs (logFilePath, ipAddress, maxDurationMinutes, maxLogs, useTLS) {
    // Logs should be ordered by timestamp asc
    const rl = readline.createInterface({
        input: fs.createReadStream(logFilePath)
    });
    const logs = []
    let oldestTimestamp = null
    let offset = null
    let count = 0

    for await (const line of rl) {
        const log = JSON.parse(line)
        log.url = new URL(log.url)
        const timestamp = new Date(log.startTime)

        // Get current timestamp offset from first/oldest log and add this
        // offset to all timestamps so they execute in the near future.
        if (oldestTimestamp === null) {
            oldestTimestamp = timestamp
            const buffer = 3000
            offset = (new Date() - oldestTimestamp) + buffer
        }
        log.startTime = new Date(timestamp.getTime() + offset)

        if (ipAddress) {
            log.url.hostname = ipAddress
        }
        log.url.protocol = useTLS ? 'https:' : 'http:'

        logs.push(log)

        if (oldestTimestamp && maxDurationMinutes) {
            const durationMinutesSoFar = (timestamp - oldestTimestamp) / 1000 / 60
            if (durationMinutesSoFar > maxDurationMinutes) {
                break
            }
        }
        if (maxLogs && count > maxLogs) {
            break
        }
        count++
    }

    return logs
}

async function replayLogs (logs, httpVersion) {
    const promises = []

    for (let i = 0; i < logs.length;) {
        const log = logs[i]
        if (log.startTime < new Date()) {
            i++
            const promise = httpVersion === 1 ? sendRequestHttp1(log) : sendRequestHttp2(log)
            promises.push(promise)

            promise.then(result => {
                const progress = ((i / logs.length) * 100).toFixed(2)
                cl(i, `${progress}%`, JSON.stringify(result))
            })
        } else {
            await setTimeout(0) // wait for next log
        }
    }

    return await Promise.all(promises)
}

function calcMetrics (results) {
    results = results.filter(d => d.status === 200)
    const metrics = []
    const groups = lodash.groupBy(results, d => `${d.status}_${d.format}_${d.cacheHit}`)

    for (const [key, values] of Object.entries(groups)) {
        const [status, format, cacheHit] = key.split('_')
        const [p50, p90, p95, p99] = percentile([50, 90, 95, 99], values.map(d => d.ttfb))
        metrics.push({
            status,
            format,
            cacheHit,
            ttfb_ms: {p50, p90, p95, p99},
            numLogs: values.length
        })
    }
    metrics.sort((a, b) => b.numLogs - a.numLogs)

    return metrics
}

async function replay ({ logFilePath, ipAddress, maxDurationMinutes, maxLogs, httpVersion, useTLS }) {
    const logs = await getModifiedLogs(logFilePath, ipAddress, maxDurationMinutes, maxLogs, useTLS)
    const results = await replayLogs(logs, httpVersion)
    const metrics = calcMetrics(results)

    if (!ipAddress) {
        ipAddress = (new URL(logs[0].url)).hostname
    }

    const info = {
        ipAddress,
        httpVersion,
        date: new Date(),
        numLogs: logs.length,
        metrics
    }

    await fsp.writeFile(`results_${Date.now()}.json`, JSON.stringify(info, null, 2))
}

async function main () {
    const argv = minimist(process.argv.slice(2))
    const cmd = argv._

    if (cmd[0] === 'get-logs') {
        const sinceMinutes = argv.since ?? 10
        await getLogs(sinceMinutes)
    } else if (cmd[0] === 'replay') {
        const logFilePath = argv.f ?? LOG_FILE
        const maxDurationMinutes = argv.d // Limit logs in the range (oldestTimestamp, oldestTimestamp + maxDurationMinutes)
        const maxLogs = argv.n
        const ipAddress = argv.ip // L1 node ip address
        const httpVersion = argv.http ?? 1 // 1 or 2
        const useTLS = argv.tls ?? true // note: http2 breaks if useTLS is false

        await replay({ logFilePath, ipAddress, maxDurationMinutes, maxLogs, httpVersion, useTLS })
    }
}

main()
