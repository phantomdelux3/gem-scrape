import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const { Pool } = pg;

// Create a pool instance
const pool = new Pool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: process.env.DB_PORT || 5432,
});

export async function initDB() {
    const client = await pool.connect();
    try {
        console.log('Validating Database Schema...');

        // 1. Organisations
        await client.query(`
      CREATE TABLE IF NOT EXISTS organisations (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        type TEXT,
        ministry TEXT,
        department TEXT,
        office_zone TEXT
      );
    `);

        // 2. Buyers (Unique constraint on unique identifier like email or phone if possible, otherwise use a composite)
        // We'll use email as primary unique key if available, else name + designation combo? 
        // User requested deduplication. Ideally email is best.
        await client.query(`
      CREATE TABLE IF NOT EXISTS buyers (
        id SERIAL PRIMARY KEY,
        name TEXT,
        designation TEXT,
        email TEXT UNIQUE,
        contact_number TEXT,
        address TEXT,
        gstin TEXT
      );
    `);

        // 3. Sellers (Unique GeM ID)
        await client.query(`
      CREATE TABLE IF NOT EXISTS sellers (
        id SERIAL PRIMARY KEY,
        gem_seller_id TEXT UNIQUE,
        company_name TEXT,
        email TEXT,
        contact_number TEXT,
        address TEXT,
        msme_reg_number TEXT,
        mse_social_category TEXT,
        mse_gender TEXT,
        gstin TEXT
      );
    `);

        // 4. Contracts
        await client.query(`
      CREATE TABLE IF NOT EXISTS contracts (
        id SERIAL PRIMARY KEY,
        contract_number TEXT UNIQUE,
        contract_date TIMESTAMP,
        total_value NUMERIC,
        status TEXT,
        buyer_id INTEGER REFERENCES buyers(id),
        seller_id INTEGER REFERENCES sellers(id),
        organisation_id INTEGER REFERENCES organisations(id),
        paying_authority_details JSONB
      );
    `);

        // 5. Products
        await client.query(`
      CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        contract_id INTEGER REFERENCES contracts(id),
        name TEXT,
        brand TEXT,
        model TEXT,
        hsn_code TEXT,
        quantity INTEGER,
        unit_price NUMERIC,
        total_price NUMERIC,
        full_details JSONB
      );
    `);

        // 6. Consignees
        await client.query(`
      CREATE TABLE IF NOT EXISTS consignees (
        id SERIAL PRIMARY KEY,
        contract_id INTEGER REFERENCES contracts(id),
        name TEXT,
        designation TEXT,
        email TEXT,
        contact TEXT,
        address TEXT,
        gstin TEXT,
        item_name TEXT,
        quantity INTEGER,
        delivery_start_date TIMESTAMP,
        delivery_end_date TIMESTAMP
      );
    `);

        console.log('Database Schema Initialized.');
    } catch (err) {
        console.error('Error initializing DB:', err);
        throw err;
    } finally {
        client.release();
    }
}

export default pool;
