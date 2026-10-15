import inquirer from 'inquirer';
import { runScraper, runExtraction, parseDate } from './scraper.js';
import { showStats, searchContracts } from './manager.js';

function isValidDate(input) {
    if (!/^\d{2}-\d{2}-\d{4}$/.test(input)) return false;
    const [d, m, y] = input.split('-').map(Number);
    const date = parseDate(input);
    return date.getDate() === d && date.getMonth() === m - 1 && date.getFullYear() === y;
}

// --- SCRAPER FUNCTION ---
async function startScraper() {
    console.log('\n--- Scraper Configuration ---');
    const answers = await inquirer.prompt([
        {
            type: 'input',
            name: 'cookie',
            message: 'Cookie string (optional, not needed for public contracts):',
            default: ''
        },
        {
            type: 'input',
            name: 'throttleTime',
            message: 'Throttling time in ms (default 1000):',
            default: '1000',
            validate: (input) => /^\d+$/.test(String(input).trim()) ? true : 'Enter a whole number',
            filter: (input) => parseInt(input, 10)
        },
        {
            type: 'input',
            name: 'fromDate',
            message: 'Enter From Date (DD-MM-YYYY):',
            default: '01-01-2024',
            validate: (input) => isValidDate(input) ? true : 'Invalid date (use DD-MM-YYYY)'
        },
        {
            type: 'input',
            name: 'toDate',
            message: 'Enter To Date (DD-MM-YYYY):',
            default: '31-03-2024',
            validate: (input, prev) => {
                if (!isValidDate(input)) return 'Invalid date (use DD-MM-YYYY)';
                return parseDate(input) >= parseDate(prev.fromDate) ? true : 'To Date must be on or after From Date';
            }
        },
        {
            type: 'input',
            name: 'windowDays',
            message: 'Days per search window (smaller = fewer 500 errors, default 1):',
            default: '1',
            validate: (input) => /^[1-9]\d*$/.test(String(input).trim()) ? true : 'Enter a number >= 1',
            filter: (input) => parseInt(input, 10)
        },
        { type: 'input', name: 'department', message: 'Department (optional):', default: '' },
        { type: 'input', name: 'bno', message: 'BNO (optional):', default: '' },
        { type: 'input', name: 'buyer_category', message: 'Buyer Category:', default: 'home_medi_pa46613086_me45117086_el71535221' },
        { type: 'confirm', name: 'repeatTillDate', message: 'Ignore To Date and scrape up to today?', default: false }
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
                await runExtraction();
            } catch (error) {
                console.error('Failed to run extraction script:', error.message);
            }
        } else if (choice === 'manage') {
            await startManager();
        }
    }
}

mainMenu();
