import fs from 'fs'
import fsp from 'fs/promises'
import readline from 'readline'
import { setTimeout } from 'timers/promises'

import fetch from 'node-fetch'
import minimist from 'minimist'
import percentile from 'percentile'
import lodash from 'lodash-es'

import * as db from './db.js'

const cl = console.log
BigInt.prototype.toJSON = function() { return Number(this) }

const logFile = './logs/logs.ndjson'
const resultsFile = './results.json'

async function getLogs (sinceMinutes) {
    db.init({
        database: 'postgres'
    })

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

        // Get offset from first/oldest log, and add this offset to all timestamps
        // so they execute in the near future.
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

        if (oldestTimestamp) {
            const durationMinutesSoFar = (timestamp - oldestTimestamp) / 1000 / 60
            if (durationMinutesSoFar > maxDurationMinutes) {
                break
            }
        }
    }

    return logs
}

async function replayLogs (logs) {
    const results = []
    const promises = []

    for (let i = 0; i < logs.length;) {
        const log = logs[i]
        if (log.startTime < new Date()) {
            i++
            const promise = sendRequest(log).then(res => {
                results.push(res)

                const progress = ((i / logs.length) * 100).toFixed(2)
                cl(i, `${progress}%`, JSON.stringify(res))
            })
            promises.push(promise)
        } else {
            await setTimeout(0)
        }
    }

    await Promise.all(promises)

    return results
}

function calcMetrics (results) {
    const metrics = []
    const groups = lodash.groupBy(results, d => `${d.status}_${d.format}_${d.cacheHit}`)

    for (const [key, values] of Object.entries(groups)) {
        const [status, format, cacheHit] = key.split('_')
        metrics.push({
            status,
            format,
            cacheHit,
            percentiles: percentile([50, 90, 95, 99], values.map(d => d.ttfb)),
            count: values.length
        })
    }
    metrics.sort((a, b) => b.count - a.count)

    return metrics
}

async function replay (ipAddress, maxDurationMinutes) {
    const logs = await getModifiedLogs(ipAddress, maxDurationMinutes)
    const results = await replayLogs(logs)

    await fsp.writeFile(resultsFile, JSON.stringify(results))

    const metrics = calcMetrics(results)

    cl(JSON.stringify(metrics, null, 2))
}

async function sendRequest (log) {
    const start = Date.now()
    let ttfb = null
    let cacheHit = false
    let requestErr = null
    let status = 0

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 1000 * 60);

    try {
        const res = await fetch(log.url,
            {
                headers: { host: 'l1s.strn.pl' }
            },
            controller
        )
        status = res.status
        cacheHit = res.headers.get('saturn-cache-status') === 'HIT'

        for await (const chunk of res.body) {
            if (!ttfb) {
                ttfb = Date.now() - start
                break
            }
        }
    } catch (err) {
        cl(err)
        requestErr = err.name + ' ' + err.message
    } finally {
        clearTimeout(timeoutId)
    }

    return {ttfb, cacheHit, status, format: log.format, requestErr}
}

async function main () {
    const argv = minimist(process.argv.slice(2))
    const cmd = argv._

    if (cmd[0] === 'get-logs') {
        const sinceMinutes = argv.since ?? 10
        await getLogs(sinceMinutes)
    } else if (cmd[0] === 'replay') {
        const durationMinutes = argv.d
        const ipAddress = argv.ip
        await replay(ipAddress, durationMinutes)
    }
}

main()
