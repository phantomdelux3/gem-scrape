import axios from 'axios';
import * as cheerio from 'cheerio';
import fs from 'fs-extra';
import path from 'path';
import qs from 'qs';
import { spawn } from 'child_process';

// Helper for delay
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const BASE_URL = 'https://gem.gov.in';
const CACHE_FILE = 'temp-cache.txt';
const FAILED_CONTRACTS_FILE = 'failed-contracts.txt';
const FAILED_RANGES_FILE = 'failed-ranges.txt';
const PDF_ROOT = 'pdfs';
const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36';

// GeM returns intermittent HTTP 500s when hit repeatedly (rate limiting / overload).
// They clear up after a pause, so every request is retried with growing back-off.
const RETRY_DELAYS_MS = [5000, 10000, 20000, 40000, 60000, 90000];
// Deep pages are slow on GeM's side (page 100 of a busy window takes ~25s).
const REQUEST_TIMEOUT_MS = 180000;
// Safety net in case the server ever ignores the page parameter.
const MAX_PAGES_PER_WINDOW = 5000;

// Date Helpers
export function parseDate(dateStr) {
    const [d, m, y] = dateStr.split('-').map(Number);
    return new Date(y, m - 1, d);
}

function formatDate(date) {
    const d = String(date.getDate()).padStart(2, '0');
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const y = date.getFullYear();
    return `${d}-${m}-${y}`;
}

function addDays(date, days) {
    const d = new Date(date);
    d.setDate(d.getDate() + days);
    return d;
}

// Split [from, to] into consecutive windows of `days` days. Small windows keep the
// page count low; GeM gets slower (and fails more) the deeper you page.
function buildWindows(from, to, days) {
    const windows = [];
    let start = from;
    while (start <= to) {
        let end = addDays(start, days - 1);
        if (end > to) end = to;
        windows.push({ from: formatDate(start), to: formatDate(end) });
        start = addDays(end, 1);
    }
    return windows;
}

function isRetryable(err) {
    if (!err.response) return true; // network error / timeout / reset
    const status = err.response.status;
    return status >= 500 || status === 429 || status === 408 || status === 403;
}

async function withRetry(label, fn) {
    for (let attempt = 0; ; attempt++) {
        try {
            return await fn();
        } catch (err) {
            if (attempt >= RETRY_DELAYS_MS.length || !isRetryable(err)) throw err;
            const wait = RETRY_DELAYS_MS[attempt] + Math.floor(Math.random() * 2000);
            const reason = err.response ? `HTTP ${err.response.status}` : err.code || err.message;
            console.warn(`  ! ${label}: ${reason} - retry ${attempt + 1}/${RETRY_DELAYS_MS.length} in ${Math.round(wait / 1000)}s`);
            await sleep(wait);
        }
    }
}

class BadResponseError extends Error {
    constructor(message) {
        super(message);
        // Treat like a 5xx so it gets retried.
        this.response = { status: 502 };
    }
}

async function fetchListPage(client, window, options, page) {
    const payload = {
        fromDate: window.from,
        toDate: window.to,
        department: options.department,
        bno: options.bno,
        buyer_category: options.buyer_category,
        page: page.toString()
    };

    return withRetry(`${window.from}..${window.to} page ${page}`, async () => {
        const response = await client.post('/view_contracts/contract_details', qs.stringify(payload), { responseType: 'text' });
        const html = String(response.data ?? '');
        if (!html.trim()) return [];

        const $ = cheerio.load(html);
        const ids = [];
        $('.block_header a[onclick]').each((i, el) => {
            const match = $(el).attr('onclick').match(/openCap\('([^']+)'\)/);
            if (match) ids.push(match[1]);
        });

        // A non-empty body without contracts is an error page, not the end of results.
        if (ids.length === 0) throw new BadResponseError('unexpected page content');
        return ids;
    });
}

async function downloadContract(client, contractId, outputDir) {
    const dlRes = await withRetry(`${contractId} link`, () =>
        client.post('/view_contracts/sbtCaptcha', qs.stringify({ oid: contractId }))
    );

    let data = dlRes.data;
    if (typeof data === 'string') {
        try { data = JSON.parse(data); } catch { throw new Error('download link response is not JSON'); }
    }
    if (!data || String(data.status) !== '1' || !data.code) {
        throw new Error(`download link status ${data?.status}`);
    }

    const downloadUrl = cheerio.load(data.code)('a').attr('href');
    if (!downloadUrl) throw new Error('no download URL in response');

    // The PDF is served from fulfilment.gem.gov.in, so use a plain GET without the gem.gov.in AJAX headers.
    const pdfRes = await withRetry(`${contractId} pdf`, async () => {
        const res = await axios.get(new URL(downloadUrl, BASE_URL).href, {
            responseType: 'arraybuffer',
            timeout: REQUEST_TIMEOUT_MS,
            headers: { 'User-Agent': USER_AGENT, 'Referer': `${BASE_URL}/` }
        });
        const buf = Buffer.from(res.data);
        if (buf.subarray(0, 5).toString('latin1') !== '%PDF-') {
            throw new BadResponseError('response is not a PDF');
        }
        return buf;
    });

    await fs.writeFile(path.join(outputDir, `${contractId}.pdf`), pdfRes);
}

async function scrapeWindow(client, window, options, outputDir, stats, cachedIds) {
    let page = 0;
    let previousPageKey = '';

    while (page < MAX_PAGES_PER_WINDOW) {
        let ids = await fetchListPage(client, window, options, page);

        if (ids.length === 0) {
            // GeM sometimes returns a short or empty page mid-stream, so confirm the end
            // by re-asking for this page and peeking at the next one.
            await sleep(3000);
            ids = await fetchListPage(client, window, options, page);
            if (ids.length === 0) {
                const next = await fetchListPage(client, window, options, page + 1);
                if (next.length === 0) break;
                console.warn(`  ! ${window.from}..${window.to} page ${page} was empty but page ${page + 1} was not - continuing`);
                page++;
                ids = next;
            }
        }

        const pageKey = ids.join(',');
        if (pageKey === previousPageKey) {
            console.warn(`  ! Page ${page} repeated the previous page - stopping this window`);
            break;
        }
        previousPageKey = pageKey;

        const newIds = ids.filter((id) => !cachedIds.has(id));
        stats.seen += ids.length;
        stats.skipped += ids.length - newIds.length;

        let pageDownloaded = 0;
        for (const contractId of newIds) {
            await sleep(options.throttleTime);
            try {
                await downloadContract(client, contractId, outputDir);
                await fs.appendFile(CACHE_FILE, `${contractId}\n`);
                cachedIds.add(contractId);
                stats.downloaded++;
                pageDownloaded++;
            } catch (err) {
                stats.failed++;
                console.error(`  x ${contractId}: ${err.message}`);
                await fs.appendFile(FAILED_CONTRACTS_FILE, `${contractId}\t${window.from}..${window.to}\t${err.message}\n`);
            }
        }

        console.log(`[${window.from}..${window.to}] page ${page}: ${ids.length} contracts, ${pageDownloaded} downloaded, ${ids.length - newIds.length} cached | total DL ${stats.downloaded}, failed ${stats.failed}`);

        page++;
        await sleep(options.throttleTime);
    }
}

export async function runExtraction() {
    // Try using venv python, fallback to system python
    const pythonCmd = await fs.pathExists('venv/Scripts/python.exe') ? 'venv\\Scripts\\python.exe' : 'python';

    // stdio: 'inherit' shows the extractor's progress live and avoids exec's output buffer limit
    await new Promise((resolve, reject) => {
        const pyProcess = spawn(pythonCmd, ['gem_extract.py'], { stdio: 'inherit' });
        pyProcess.on('close', (code) => {
            if (code === 0) resolve();
            else reject(new Error(`Extraction process exited with code ${code}`));
        });
        pyProcess.on('error', reject);
    });
}

export async function runScraper(answers) {
    const { cookie, throttleTime, department, bno, buyer_category, repeatTillDate, windowDays } = answers;

    const headers = {
        'User-Agent': USER_AGENT,
        'Accept': '*/*',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': BASE_URL,
        'Referer': `${BASE_URL}/view_contracts`
    };
    // The public contract search doesn't need a login; a cookie is only sent if given.
    if (cookie) headers.Cookie = cookie;

    const client = axios.create({ baseURL: BASE_URL, timeout: REQUEST_TIMEOUT_MS, headers });

    // Setup Cache
    const cachedIds = new Set();
    await fs.ensureDir(PDF_ROOT);
    if (await fs.pathExists(CACHE_FILE)) {
        const content = await fs.readFile(CACHE_FILE, 'utf-8');
        content.split('\n').map(l => l.trim()).filter(Boolean).forEach(id => cachedIds.add(id));
    }
    console.log(`Loaded ${cachedIds.size} cached contracts.`);

    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const from = parseDate(answers.fromDate);
    let to = repeatTillDate ? today : parseDate(answers.toDate);
    if (to > today) to = today;

    const windows = buildWindows(from, to, windowDays);
    const outputDir = path.join(PDF_ROOT, `${formatDate(from)}_to_${formatDate(to)}`);
    await fs.ensureDir(outputDir);
    console.log(`Scraping ${formatDate(from)} to ${formatDate(to)} in ${windows.length} window(s) of ${windowDays} day(s). Saving to: ${outputDir}`);

    const stats = { seen: 0, downloaded: 0, skipped: 0, failed: 0, failedWindows: 0 };
    const options = { throttleTime, department, bno, buyer_category };

    for (const window of windows) {
        try {
            await scrapeWindow(client, window, options, outputDir, stats, cachedIds);
        } catch (err) {
            // Retries exhausted: record the window so it can be re-run, and keep going.
            stats.failedWindows++;
            console.error(`\nWindow ${window.from}..${window.to} failed after retries: ${err.message}`);
            await fs.appendFile(FAILED_RANGES_FILE, `${window.from}\t${window.to}\t${buyer_category}\t${err.message}\n`);
            await sleep(60000);
        }
    }

    console.log('\nScraping completed.');
    console.log('Stats:', stats);
    if (stats.failed) console.log(`Failed contracts listed in ${FAILED_CONTRACTS_FILE} (re-running the same range retries them).`);
    if (stats.failedWindows) console.log(`Incomplete date windows listed in ${FAILED_RANGES_FILE} - re-run those dates.`);

    console.log('\nStarting data extraction...');
    try {
        await runExtraction();
    } catch (error) {
        console.error('Failed to run extraction script:', error.message);
    }

    console.log('\nAll done!');
}
