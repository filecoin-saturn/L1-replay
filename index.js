import { once } from "node:events";
import fs from 'node:fs'
import fsp from 'node:fs/promises'
import https from "node:https"
import http2 from "node:http2"
import readline from 'node:readline'
import { setTimeout } from 'node:timers/promises'

import fetch from 'node-fetch'
import minimist from 'minimist'
import percentile from 'percentile'
import lodash from 'lodash-es'

import * as db from './db.js'

BigInt.prototype.toJSON = function() { return Number(this) }
const cl = console.log
const L1S_HOST = 'l1s.strn.pl'
const httpsAgent = new https.Agent({
    servername: L1S_HOST,
})

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

async function sendRequestHttp1 (log) {
    const start = Date.now()
    let ttfb = null
    let cacheHit = false
    let requestErr = null
    let status = 0

    const controller = new AbortController();
    setTimeout(1000 * 60).then(() => controller.abort())

    try {
        const res = await fetch(log.url,
            {
                headers: { host: L1S_HOST },
                agent: httpsAgent,
                signal: controller.signal
            },
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
    }

    return {ttfb, cacheHit, status, format: log.format, requestErr}
}

async function sendRequestHttp2 (log) {
    const start = Date.now()
    let ttfb = null
    let cacheHit = false
    let requestErr = null
    let status = 0

    const url = new URL(log.url)
    const controller = new AbortController();
    const timeout = 1000 * 60
    setTimeout(timeout).then(() => controller.abort())

    const client = http2.connect(url.origin, { servername: L1S_HOST });
    const req = client.request(
        {
            ":path": url.pathname + url.search,
            host: L1S_HOST,
        },
        {
            endStream: true,
            signal: controller.signal,
        }
    );

    const errHandler = async () => {
        const [err] = await once(client, "error");
        throw err;
    };

    try {
        const [headers] = await Promise.race([once(req, "response"), errHandler()]);
        status = headers[":status"];
        cacheHit = headers['saturn-cache-status'] === 'HIT'

        for await (const chunk of req) {
            if (!ttfb) {
                ttfb = Date.now() - start
                break
            }
        }
    } catch (err) {
        requestErr = err.name + ' ' + err.message
    }

    return {ttfb, cacheHit, status, format: log.format, requestErr}
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
