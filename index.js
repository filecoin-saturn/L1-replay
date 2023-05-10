
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

const logFile = './logs/logs.ndjson'
const resultsFile = './results.json'

async function getLogs (sinceMinutes) {
    db.init()

    const res = await db.getCabooseRequests(sinceMinutes)
    const wstream = fs.createWriteStream(logFile)
    for (const row of res) {
        wstream.write(JSON.stringify(row) + '\n')
    }
    wstream.end()
}

// Get logs with past timestamps modified into future timestamps
async function getModifiedLogs (ipAddress, maxDurationMinutes) {
    // Logs should be ordered by timestamp asc
    const rl = readline.createInterface({
        input: fs.createReadStream(logFile)
    });
    const logs = []
    let oldestTimestamp = null
    let offset = null

    for await (const line of rl) {
        const log = JSON.parse(line)
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
            const url = new URL(log.url)
            url.hostname = ipAddress
            log.url = url
        }

        logs.push(log)

        if (oldestTimestamp && maxDurationMinutes) {
            const durationMinutesSoFar = (timestamp - oldestTimestamp) / 1000 / 60
            if (durationMinutesSoFar > maxDurationMinutes) {
                break
            }
        }
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
            await setTimeout(0)
        }
    }

    const results = await Promise.all(promises)

    return results
}

function calcPercentiles (results) {
    const percentiles = []
    const groups = lodash.groupBy(results, d => `${d.status}_${d.format}_${d.cacheHit}`)

    for (const [key, values] of Object.entries(groups)) {
        const [status, format, cacheHit] = key.split('_')
        percentiles.push({
            status,
            format,
            cacheHit,
            percentiles: percentile([50, 90, 95, 99], values.map(d => d.ttfb)),
            count: values.length
        })
    }
    percentiles.sort((a, b) => b.count - a.count)

    return percentiles
}

async function replay (ipAddress, maxDurationMinutes, httpVersion) {
    const logs = await getModifiedLogs(ipAddress, maxDurationMinutes)
    const results = await replayLogs(logs, httpVersion)
    const percentiles = calcPercentiles(results)

    if (!ipAddress) {
        ipAddress = (new URL(logs[0].url)).hostname
    }

    const info = {
        ipAddress,
        httpVersion,
        percentiles
    }

    await fsp.writeFile(resultsFile, JSON.stringify(info, null, 2))
}

async function main () {
    const argv = minimist(process.argv.slice(2))
    const cmd = argv._

    if (cmd[0] === 'get-logs') {
        const sinceMinutes = argv.since ?? 10
        await getLogs(sinceMinutes)
    } else if (cmd[0] === 'replay') {
        const maxDurationMinutes = argv.d
        const ipAddress = argv.ip
        const httpVersion = argv.http ?? 1 // 1 or 2
        await replay(ipAddress, maxDurationMinutes, httpVersion)
    }
}

main()
