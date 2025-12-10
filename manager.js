import inquirer from 'inquirer';
import { default as pool } from './db.js';

export async function showStats() {
    const client = await pool.connect();
    try {
        console.log('\n--- Database Statistics ---');

        const contractCount = await client.query('SELECT COUNT(*) FROM contracts');
        const buyerCount = await client.query('SELECT COUNT(*) FROM buyers');
        const sellerCount = await client.query('SELECT COUNT(*) FROM sellers');
        const orgCount = await client.query('SELECT COUNT(*) FROM organisations');
        const productCount = await client.query('SELECT COUNT(*) FROM products');

        console.table({
            'Contracts': contractCount.rows[0].count,
            'Unique Buyers': buyerCount.rows[0].count,
            'Unique Sellers': sellerCount.rows[0].count,
            'Organisations': orgCount.rows[0].count,
            'Products': productCount.rows[0].count
        });

    } catch (err) {
        console.error('Error fetching stats:', err.message);
    } finally {
        client.release();
    }
}

export async function searchContracts() {
    console.log('\n--- Search Contracts ---');
    const { term } = await inquirer.prompt([
        { type: 'input', name: 'term', message: 'Enter search term (Contract No, Org, Buyer, Seller):' }
    ]);

    if (!term) return;

    const client = await pool.connect();
    try {
        const query = `
            SELECT 
                c.contract_number,
                TO_CHAR(c.contract_date, 'DD-MM-YYYY') as date,
                o.name as org_name,
                b.designation as buyer_desig,
                s.company_name as seller_name,
                c.total_value
            FROM contracts c
            LEFT JOIN organisations o ON c.organisation_id = o.id
            LEFT JOIN buyers b ON c.buyer_id = b.id
            LEFT JOIN sellers s ON c.seller_id = s.id
            WHERE 
                c.contract_number ILIKE $1 OR
                o.name ILIKE $1 OR
                b.name ILIKE $1 OR
                b.designation ILIKE $1 OR
                s.company_name ILIKE $1
            LIMIT 20
        `;

        const res = await client.query(query, [`%${term}%`]);

        if (res.rows.length === 0) {
            console.log('No matches found.');
        } else {
            console.table(res.rows);
            console.log(`Showing ${res.rows.length} results.`);
        }

    } catch (err) {
        console.error('Search error:', err.message);
    } finally {
        client.release();
    }
}
