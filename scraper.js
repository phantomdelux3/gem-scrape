import axios from 'axios';
import * as cheerio from 'cheerio';
import inquirer from 'inquirer';
import fs from 'fs-extra';
import path from 'path';
import qs from 'qs';
import { SingleBar, Presets } from 'cli-progress';

// Helper for delay
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const BASE_URL = 'https://gem.gov.in';
const CACHE_FILE = 'temp-cache.txt';
const PDF_ROOT = 'pdfs';

// Date Helpers
function parseDate(dateStr) {
    const [d, m, y] = dateStr.split('-').map(Number);
    return new Date(y, m - 1, d);
}

function formatDate(date) {
    const d = String(date.getDate()).padStart(2, '0');
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const y = date.getFullYear();
    return `${d}-${m}-${y}`;
}

function addMonths(date, months) {
    const d = new Date(date);
    d.setMonth(d.getMonth() + months);
    if (d.getDate() !== date.getDate()) {
        d.setDate(0);
    }
    return d;
}

function addDays(date, days) {
    const d = new Date(date);
    d.setDate(d.getDate() + days);
    return d;
}

async function scrapeRange(client, fromDateStr, toDateStr, options, stats, cachedIds, progressBar) {
    const rangeFolder = `${fromDateStr}_to_${toDateStr}`;
    const outputDir = path.join(PDF_ROOT, rangeFolder);
    await fs.ensureDir(outputDir);

    console.log(`\n\n--- Scraping Period: ${fromDateStr} to ${toDateStr} ---`);
    console.log(`Saving to: ${outputDir}`);

    let page = 0;
    let hasNext = true;

    while (hasNext) {
        try {
            const payload = {
                fromDate: fromDateStr,
                toDate: toDateStr,
                department: options.department,
                bno: options.bno,
                buyer_category: options.buyer_category,
                page: page.toString()
            };

            const response = await client.post('/view_contracts/contract_details', qs.stringify(payload));

            const $ = cheerio.load(response.data);
            const contractNodes = $('.block_header');

            if (contractNodes.length === 0) {
                hasNext = false;
                break;
            }

            const contractsOnPage = [];
            contractNodes.each((i, el) => {
                const onclick = $(el).find('a').attr('onclick');
                if (onclick) {
                    const match = onclick.match(/'([^']+)'/);
                    if (match && match[1]) {
                        contractsOnPage.push(match[1]);
                    }
                }
            });

            if (contractsOnPage.length === 0) {
                hasNext = false;
                break;
            }

            progressBar.setTotal(stats.scraped + contractsOnPage.length + 50);

            for (const contractId of contractsOnPage) {
                stats.scraped++;

                if (cachedIds.has(contractId)) {
                    stats.skipped++;
                    progressBar.increment(1, {
                        total: stats.scraped + 10,
                        downloaded: stats.downloaded,
                        speed: 'Cached'
                    });
                    continue;
                }

                try {
                    // Throttling
                    await sleep(options.throttleTime);

                    const startDl = Date.now();

                    const dlPayload = { oid: contractId };
                    const dlRes = await client.post('/view_contracts/sbtCaptcha', qs.stringify(dlPayload));

                    if (dlRes.data && dlRes.data.status === '1') {
                        let downloadUrl = '';
                        const hrefMatch = dlRes.data.code.match(/href=\\?"([^"]+)\\?"/);

                        if (hrefMatch && hrefMatch[1]) {
                            downloadUrl = hrefMatch[1];
                            downloadUrl = downloadUrl.replace(/\\\//g, '/');
                        } else {
                            const $link = cheerio.load(dlRes.data.code);
                            downloadUrl = $link('a').attr('href');
                        }

                        if (downloadUrl) {
                            const pdfRes = await client.get(downloadUrl, { responseType: 'arraybuffer' });
                            await fs.writeFile(path.join(outputDir, `${contractId}.pdf`), pdfRes.data);

                            await fs.appendFile(CACHE_FILE, `${contractId}\n`);
                            cachedIds.add(contractId);
                            stats.downloaded++;

                            const dlTime = Date.now() - startDl;
                            progressBar.increment(1, {
                                total: stats.scraped + 10,
                                downloaded: stats.downloaded,
                                speed: `${dlTime}ms`
                            });
                        } else {
                            stats.failed++;
                            progressBar.increment(1, { speed: 'NoURL' });
                        }
                    } else {
                        stats.failed++;
                        progressBar.increment(1, { speed: 'ErrStatus' });
                    }

                } catch (err) {
                    stats.failed++;
                    progressBar.increment(1, { speed: 'Error' });
                }
            }

            page++;
            await sleep(1000);

        } catch (err) {
            console.error(`\nError scraping page: ${err.message}`);
            hasNext = false;
        }
    }
}

export async function runScraper(answers) {
    const { cookie, throttleTime, department, bno, buyer_category, repeatTillDate } = answers;
    let { fromDate: currentFromStr, toDate: currentToStr } = answers;

    // Setup Axios
    const client = axios.create({
        baseURL: BASE_URL,
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'https://gem.gov.in',
            'Referer': 'https://gem.gov.in/view_contracts',
            'Cookie': cookie
        }
    });

    // Setup Cache
    const cachedIds = new Set();
    await fs.ensureDir(PDF_ROOT); // Ensure root exists
    if (await fs.pathExists(CACHE_FILE)) {
        const content = await fs.readFile(CACHE_FILE, 'utf-8');
        content.split('\n').map(l => l.trim()).filter(Boolean).forEach(id => cachedIds.add(id));
    }
    console.log(`Loaded ${cachedIds.size} cached contracts.`);

    // Progress Bar
    const stats = { scraped: 0, downloaded: 0, skipped: 0, failed: 0 };
    const progressBar = new SingleBar({
        format: 'Contract | {bar} | {value} Scraped | DL: {downloaded} | Speed: {speed}',
        hideCursor: true
    }, Presets.shades_classic);
    progressBar.start(100, 0, { downloaded: 0, speed: 'N/A' });

    // Processing Loop
    const today = new Date();
    let done = false;

    while (!done) {
        // Perform scrape for current range
        await scrapeRange(
            client,
            currentFromStr,
            currentToStr,
            { throttleTime, department, bno, buyer_category },
            stats,
            cachedIds,
            progressBar
        );

        if (!repeatTillDate) {
            done = true;
        } else {
            const lastEndDate = parseDate(currentToStr);
            const nextStartDate = addDays(lastEndDate, 1);

            if (nextStartDate > today) {
                done = true;
                break;
            }

            let nextEndDate = addMonths(nextStartDate, 3);
            nextEndDate = addDays(nextEndDate, -1);

            if (nextEndDate > today) {
                nextEndDate = today;
            }

            currentFromStr = formatDate(nextStartDate);
            currentToStr = formatDate(nextEndDate);

            if (nextStartDate > nextEndDate) {
                done = true;
            }
        }
    }

    progressBar.stop();
    console.log('\nAll done!');
    console.log('Stats:', stats);
}
