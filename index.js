import inquirer from 'inquirer';
import { processAllPdfs } from './extractor.js';
import { runScraper } from './scraper.js';
import { showStats, searchContracts } from './manager.js';
import fs from 'fs-extra';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

// --- SCRAPER FUNCTION ---
async function startScraper() {
    console.log('\n--- Scraper Configuration ---');
    const answers = await inquirer.prompt([
        {
            type: 'input',
            name: 'cookie',
            message: 'Enter the Cookie string:',
            validate: (input) => input ? true : 'Cookie cannot be empty'
        },
        {
            type: 'input',
            name: 'throttleTime',
            message: 'Throttling time in ms (default 500):',
            default: '500',
            filter: (input) => parseInt(input, 10)
        },
        {
            type: 'input',
            name: 'fromDate',
            message: 'Enter From Date (DD-MM-YYYY):',
            default: '01-01-2024',
            validate: (input) => /^\d{2}-\d{2}-\d{4}$/.test(input) ? true : 'Invalid format'
        },
        {
            type: 'input',
            name: 'toDate',
            message: 'Enter To Date (DD-MM-YYYY):',
            default: '31-03-2024',
            validate: (input) => /^\d{2}-\d{2}-\d{4}$/.test(input) ? true : 'Invalid format'
        },
        { type: 'input', name: 'department', message: 'Department (optional):', default: '' },
        { type: 'input', name: 'bno', message: 'BNO (optional):', default: '' },
        { type: 'input', name: 'buyer_category', message: 'Buyer Category:', default: 'home_medi_pa46613086_me45117086_el71535221' },
        { type: 'confirm', name: 'repeatTillDate', message: 'Repeat query till date?', default: false }
    ]);

    // Call the external scraper module
    await runScraper(answers);
}

async function startManager() {
    while (true) {
        console.log('\n--- Data Management ---');
        const { action } = await inquirer.prompt([{
            type: 'list',
            name: 'action',
            message: 'Select Action:',
            choices: [
                { name: '1. View Database Stats (Count)', value: 'stats' },
                { name: '2. Search Contracts', value: 'search' },
                { name: '3. Back to Main Menu', value: 'back' }
            ]
        }]);

        if (action === 'back') break;
        if (action === 'stats') await showStats();
        if (action === 'search') await searchContracts();
    }
}

async function mainMenu() {
    while (true) {
        console.log('\n--- GeM Tool Main Menu ---');
        const { choice } = await inquirer.prompt([{
            type: 'list',
            name: 'choice',
            message: 'Select Action:',
            choices: [
                { name: '1. Scrape Contracts', value: 'scrape' },
                { name: '2. Extract Data (PDF -> Excel)', value: 'extract' },
                { name: '3. Manage Data (Stats / Search)', value: 'manage' },
                { name: '4. Exit', value: 'exit' }
            ]
        }]);

        if (choice === 'exit') process.exit(0);

        if (choice === 'scrape') {
            await startScraper();
        } else if (choice === 'extract') {
            console.log('\nStarting extraction...');
            try {
                // Try using venv python, fallback to system python
                const pythonCmd = await fs.pathExists('venv/Scripts/python.exe') ? 'venv\\Scripts\\python.exe' : 'python';
                const { stdout, stderr } = await execAsync(`${pythonCmd} gem_extract.py`);
                console.log(stdout);
                if (stderr) console.error('Extraction Errors:', stderr);
            } catch (error) {
                console.error('Failed to run extraction script:', error.message);
            }
        } else if (choice === 'manage') {
            await startManager();
        }
    }
}

mainMenu();
