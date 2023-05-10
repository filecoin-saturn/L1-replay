import { once } from "node:events";
import https from "node:https"
import http2 from "node:http2"
import { setTimeout } from 'node:timers/promises'

import fetch from 'node-fetch'

const L1S_HOST = 'l1s.strn.pl'
const httpsAgent = new https.Agent({
    servername: L1S_HOST,
})

export async function sendRequestHttp1 (log) {
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
        requestErr = err.name + ' ' + err.message
    }

    return {ttfb, cacheHit, status, format: log.format, requestErr}
}

export async function sendRequestHttp2 (log) {
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
