import fs from 'fs-extra';
import path from 'path';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const pdf = require('pdf-parse'); // Safer for CJS modules in ESM
import ExcelJS from 'exceljs';
import { initDB, default as pool } from './db.js';

// Regex patterns for extraction
// Note: We'll do a two-pass approach:
// 1. Clean the text (remove non-ASCII/Hindi).
// 2. Run regex on cleaner English text.

function cleanText(text) {
    // Remove non-ascii characters (Hindi range is mostly 0900-097F)
    // We keep basic punctuation, numbers, and English letters.
    // ASCII 32-126 are standard printable characters. 10 is newline. 13 is CR.
    return text.replace(/[^\x00-\x7F]/g, "").replace(/\s+/g, ' ').trim();
}

async function extractFromPdf(filePath) {
    const dataBuffer = await fs.readFile(filePath);
    const data = await pdf(dataBuffer);
    const rawText = data.text;

    // Normalize whitespace to single spaces for easier regex
    const text = cleanText(rawText);

    // LOGIC: The text layout in PDF parse is linear.
    // We look for keywords like "Organisation Details", "Buyer Details", "Financial Approval Detail", "Seller Details", "Product Details", "Consignee Detail"

    // Helper to extract between two markers
    const extractBetween = (startStr, endStr) => {
        const startIndex = text.indexOf(startStr);
        if (startIndex === -1) return '';
        const remainder = text.substring(startIndex + startStr.length);
        if (!endStr) return remainder;
        const endIndex = remainder.indexOf(endStr);
        if (endIndex === -1) return remainder; // till end
        return remainder.substring(0, endIndex).trim();
    };

    // 1. Contract No & Date
    // "Contract NO: GEMC-..."
    const contractNoMatch = text.match(/Contract NO:\s*([A-Za-z0-9-]+)/i);
    const contractDateMatch = text.match(/Contract Date:\s*([\d\/:\s]+)/i);

    const contractNo = contractNoMatch ? contractNoMatch[1] : path.basename(filePath, '.pdf');
    const contractDate = contractDateMatch ? contractDateMatch[1].trim() : '';

    // 2. Organisation Details
    // Starts with "Organisation Details" ends with "Buyer Details"?
    // Or look specific fields: "Type :", "Ministry :", "Department :", "Organisation Name :", "Office Zone :"
    const orgType = (text.match(/Type\s*:\s*([^:]+?)\s*(Ministry|Department)/) || [])[1]?.trim();
    const ministry = (text.match(/Ministry\s*:\s*([^:]+?)\s*(Department|Organisation)/) || [])[1]?.trim();
    const department = (text.match(/Department\s*:\s*([^:]+?)\s*(Organisation)/) || [])[1]?.trim();
    const orgName = (text.match(/Organisation\s*Name\s*:\s*([^:]+?)\s*(Office Zone)/) || [])[1]?.trim();
    const officeZone = (text.match(/Office Zone\s*:\s*([^:]+?)\s*(Buyer Details)/) || [])[1]?.trim();

    // 3. Buyer Details
    const buyerSection = extractBetween("Buyer Details", "Paying Authority Details");
    const buyerDesignation = (buyerSection.match(/Designation\s*:\s*([^:]+?)\s*(Contact|Email)/) || [])[1]?.trim();
    const buyerEmail = (buyerSection.match(/Email ID\s*:\s*([^:]+?)\s*(GSTIN|Address)/) || [])[1]?.trim();
    const buyerContact = (buyerSection.match(/Contact No\.\s*:\s*([\d-]+)/) || [])[1]?.trim();
    const buyerAddress = (buyerSection.match(/Address\s*:\s*(.+)$/) || [])[1]?.trim(); // simplified
    const buyerName = "N/A"; // Usually not explicitly "Name :", often Designaton is prominent. User showed "Buyer Designing: Store keeper"

    // 4. Seller Details
    const sellerSection = extractBetween("Seller Details", "Product Details"); // or specific end
    const gemSellerId = (sellerSection.match(/GeM Seller ID\s*:\s*([A-Z0-9]+)/) || [])[1]?.trim();
    const companyName = (sellerSection.match(/Company Name\s*:\s*([^:]+?)\s*(Contact|Email)/) || [])[1]?.trim();
    const sellerEmail = (sellerSection.match(/Email ID\s*:\s*([^:]+?)\s*(Address)/) || [])[1]?.trim();
    const sellerContact = (sellerSection.match(/Contact No\.\s*:\s*([\d-]+)/) || [])[1]?.trim();
    const sellerAddress = (text.match(/Address\s*:\s*([^:]+?)\s*(MSME|MSE)/) || [])[1]?.trim(); // might need tuning if multiple addresses
    // Actually Seller section has its own address block. 

    // 5. Products & Totals
    // Based on user sample, text might be: 
    // "1 Product Name : ... Brand : ... ... Ordered Quantity ... Unit Price ... Tax ... Price (Inclusive...)"
    // The "Product Name :" is a good delimiter.

    const products = [];
    let totalQty = 0;

    // Split text by "Product Name :" to isolate blocks
    // User sample: "1 Product Name : Unbranded ... Brand : NA ... Ordered Quantity ... Unit Price ... Total Order Value"
    const productBlocks = text.split(/Product Name\s*:/i);

    if (productBlocks.length > 1) {
        for (let i = 1; i < productBlocks.length; i++) {
            const block = productBlocks[i];

            // Extract fields from the block
            // Note: The fields might be separated by newlines in raw text which become spaces in cleanText.
            // Example: "... Brand : NA ... Unit Price (INR) ... 1,810 ... "

            const name = block.split(/Brand\s*:/i)[0].trim();
            const brand = (block.match(/Brand\s*:\s*([^:]+?)\s*Brand/i) || [])[1]?.trim() || "NA";
            const model = (block.match(/Model\s*:\s*([^:]+?)\s*(HSN|Ordered|Category)/i) || [])[1]?.trim() || "NA";

            // Quantity: "Ordered Quantity ... 4"
            // There might be extra text in between "Ordered Quantity" and the number if it's a table header row
            // Regex: Look for "Ordered Quantity" then digits.
            const qtyMatch = block.match(/Ordered\s*Quantity\s*[^\d]*(\d+)/i);
            const qty = qtyMatch ? parseInt(qtyMatch[1], 10) : 0;
            totalQty += qty;

            // Unit Price: "Unit Price (INR) ... 1,810"
            // Regex: "Unit Price" ... digits/commas/dots
            const unitPriceMatch = block.match(/Unit\s*Price\s*\(INR\)\s*[^\d]*([\d,.]+)/i);
            const unitPrice = unitPriceMatch ? unitPriceMatch[1].replace(/,/g, '').trim() : "0";

            products.push({
                name, brand, model, quantity: qty, unitPrice
            });
        }
    }

    // Total Value of Contract
    // "Total Order Value (in INR) 7,240"
    const totalValueMatch = text.match(/Total\s*(Order)?\s*Value\s*\(?in\s*INR\)?\s*([\d,.]+)/i);
    const totalValue = totalValueMatch ? totalValueMatch[2].replace(/,/g, '') : "0";

    // 6. Consignee
    // "Consignee Detail ... 1 Consignee Item Lot No. Quantity ... "
    // User wants structured data but text is messy.
    // Let's at least clean it up nicely.
    const consigneeSection = extractBetween("Consignee Detail", "Terms and Conditions");

    // Attempt to extract fields if possible, otherwise clean text
    const cleanConsignee = consigneeSection.replace(/\s+/g, ' ').trim();

    // Try to find the name: "Designation : ... " is often inside the block
    // User sample: "Designation : Assistant Director Fisheries ... Email ID ... Address ..."
    // We can extract these specific sub-fields to be helpful.
    const consName = (consigneeSection.match(/Designation\s*:\s*([^:]+?)\s*(Email|Contact)/i) || [])[1]?.trim() || "N/A";
    const consEmail = (consigneeSection.match(/Email\s*ID\s*:\s*([^:]+?)\s*(Contact|GSTIN)/i) || [])[1]?.trim() || "N/A";
    const consAddress = (consigneeSection.match(/Address\s*:\s*(.+)$/i) || [])[1]?.trim() || "N/A";

    return {
        contract: {
            number: contractNo,
            date: contractDate,
            totalValue: totalValue,
            totalQuantity: totalQty
        },
        org: {
            type: orgType,
            ministry,
            department,
            name: orgName,
            zone: officeZone
        },
        buyer: {
            designation: buyerDesignation,
            email: buyerEmail,
            contact: buyerContact,
            address: buyerAddress
        },
        seller: {
            gemId: gemSellerId,
            name: companyName,
            email: sellerEmail,
            contact: sellerContact
        },
        products,
        consignees: [{
            name: consName,
            email: consEmail,
            address: consAddress,
            fullText: cleanConsignee
        }]
    };
}

export async function processAllPdfs(pdfDir, exportRaw = true, exportDb = false) {
    console.log(`[DEBUG] Starting processAllPdfs. Dir: ${pdfDir}, ExportExcel: ${exportRaw}, ExportDB: ${exportDb}`);

    if (!exportRaw && !exportDb) {
        console.warn('[WARN] No export target selected (Excel or DB). Nothing to do.');
        return;
    }

    // Collect all PDF paths
    const pdfFiles = [];
    async function traverse(dir) {
        try {
            const files = await fs.readdir(dir);
            for (const f of files) {
                const fullPath = path.join(dir, f);
                const stat = await fs.stat(fullPath);
                if (stat.isDirectory()) {
                    await traverse(fullPath);
                } else if (f.toLowerCase().endsWith('.pdf')) {
                    pdfFiles.push(fullPath);
                }
            }
        } catch (e) {
            console.error(`[ERROR] Error traversing directory ${dir}:`, e.message);
        }
    }

    if (!await fs.pathExists(pdfDir)) {
        console.error(`[ERROR] Directory not found: ${pdfDir}`);
        return;
    }

    await traverse(pdfDir);

    console.log(`[DEBUG] Found ${pdfFiles.length} PDFs to process.`);
    if (pdfFiles.length === 0) {
        console.log('[WARN] No PDFs found. Exiting extraction.');
        return;
    }

    // Setup Excel
    let workbook, contractsSheet, productsSheet, consigneesSheet;
    if (exportRaw) {
        try {
            console.log('[DEBUG] Initializing Excel Workbook...');
            await fs.ensureDir('data_exports');
            workbook = new ExcelJS.Workbook();

            // Sheet 1: Contracts (Main info)
            contractsSheet = workbook.addWorksheet('Contracts');
            contractsSheet.columns = [
                { header: 'Contract No', key: 'cno' },
                { header: 'Contract Date', key: 'cdate' },
                { header: 'Total Value (INR)', key: 'tval' },      // NEW
                { header: 'Total Quantity', key: 'tqty' },        // NEW
                { header: 'Organisation Name', key: 'org_name' },
                { header: 'Ministry', key: 'ministry' },
                { header: 'Department', key: 'dept' },
                { header: 'Buyer Designation', key: 'b_desig' },
                { header: 'Buyer Email', key: 'b_email' },
                { header: 'Buyer Contact', key: 'b_contact' },
                { header: 'Buyer Address', key: 'b_addr' },
                { header: 'Seller Name', key: 's_name' },
                { header: 'Seller GeM ID', key: 's_id' },
                { header: 'Seller Email', key: 's_email' },
                { header: 'Seller Contact', key: 's_contact' }
            ];

            // Sheet 2: Products
            productsSheet = workbook.addWorksheet('Products');
            productsSheet.columns = [
                { header: 'Contract No', key: 'cno' },
                { header: 'Product Name', key: 'p_name' },
                { header: 'Brand', key: 'brand' },
                { header: 'Model', key: 'model' },
                { header: 'Quantity', key: 'qty' },
                { header: 'Unit Price', key: 'u_price' }          // NEW
            ];

            // Sheet 3: Consignees
            consigneesSheet = workbook.addWorksheet('Consignees');
            consigneesSheet.columns = [
                { header: 'Contract No', key: 'cno' },
                { header: 'Consignee Name', key: 'name' },
                { header: 'Consignee Email', key: 'email' },
                { header: 'Consignee Address', key: 'addr' },
                { header: 'Full Text Details', key: 'details' }
            ];
            console.log('[DEBUG] Excel Workbook initialized.');
        } catch (e) {
            console.error('[ERROR] Failed to initialize Excel:', e);
        }
    }

    // Setup DB
    if (exportDb) {
        await initDB();
    }

    let processedCount = 0;
    for (const file of pdfFiles) {
        try {
            // console.log(`[DEBUG] Processing: ${path.basename(file)}`);
            const data = await extractFromPdf(file);

            // Excel
            if (exportRaw && workbook) {
                // Add to Contracts Sheet
                contractsSheet.addRow({
                    cno: data.contract.number,
                    cdate: data.contract.date,
                    tval: data.contract.totalValue,    // NEW
                    tqty: data.contract.totalQuantity, // NEW
                    org_name: data.org.name,
                    ministry: data.org.ministry,
                    dept: data.org.department,
                    b_desig: data.buyer.designation,
                    b_email: data.buyer.email,
                    b_contact: data.buyer.contact,
                    b_addr: data.buyer.address,
                    s_name: data.seller.name,
                    s_id: data.seller.gemId,
                    s_email: data.seller.email,
                    s_contact: data.seller.contact
                });

                // Add to Products Sheet
                for (const prod of data.products) {
                    productsSheet.addRow({
                        cno: data.contract.number,
                        p_name: prod.name,
                        brand: prod.brand,
                        model: prod.model,
                        qty: prod.quantity,
                        u_price: prod.unitPrice         // NEW
                    });
                }

                // Add to Consignees Sheet
                for (const consignee of data.consignees) {
                    consigneesSheet.addRow({
                        cno: data.contract.number,
                        name: consignee.name,
                        email: consignee.email,     // NEW
                        addr: consignee.address,    // NEW
                        details: consignee.fullText
                    });
                }
            }

            // DB
            if (exportDb) {
                const client = await pool.connect();
                try {
                    await client.query('BEGIN');

                    // 1. Org
                    const orgRes = await client.query(`
                        INSERT INTO organisations (name, type, ministry, department, office_zone)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (name) DO UPDATE SET type=EXCLUDED.type
                        RETURNING id
                    `, [data.org.name, data.org.type, data.org.ministry, data.org.department, data.org.zone]);
                    const orgId = orgRes.rows[0].id;

                    // 2. Buyer (Dedupe by email)
                    let buyerId;
                    if (data.buyer.email) {
                        const buyerRes = await client.query(`
                            INSERT INTO buyers (designation, email, contact_number, address)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (email) DO UPDATE SET designation=EXCLUDED.designation
                            RETURNING id
                        `, [data.buyer.designation, data.buyer.email, data.buyer.contact, data.buyer.address]);
                        buyerId = buyerRes.rows[0].id;
                    }

                    // 3. Seller (Dedupe by GeM ID)
                    let sellerId;
                    if (data.seller.gemId) {
                        const sellerRes = await client.query(`
                            INSERT INTO sellers (gem_seller_id, company_name, email, contact_number)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (gem_seller_id) DO UPDATE SET company_name=EXCLUDED.company_name
                            RETURNING id
                        `, [data.seller.gemId, data.seller.name, data.seller.email, data.seller.contact]);
                        sellerId = sellerRes.rows[0].id;
                    }

                    // 4. Contract
                    if (data.contract.number) {
                        // Check if exists
                        const checkRes = await client.query('SELECT id FROM contracts WHERE contract_number = $1', [data.contract.number]);
                        let contractId;

                        if (checkRes.rows.length === 0) {
                            const cRes = await client.query(`
                                INSERT INTO contracts (contract_number, contract_date, organisation_id, buyer_id, seller_id)
                                VALUES ($1, TO_TIMESTAMP($2, 'DD/MM/YYYY HH24:MI'), $3, $4, $5)
                                RETURNING id
                            `, [data.contract.number, data.contract.date, orgId, buyerId, sellerId]);
                            contractId = cRes.rows[0].id;
                        } else {
                            contractId = checkRes.rows[0].id;
                        }

                        // 5. Products (Insert if new contract or append?)
                        // To avoid duplicates on re-run, ideally delete old products for this contract or check existence.
                        // For now, simpler: only insert if contract was new (handled by flow above implicitly if we assume products transform is idempotent)
                        // Actually, if contract exists, we skip contract insert, but we should probably verify products. 
                        // Let's keep it simple: insert products only if we just inserted the contract.
                        if (checkRes.rows.length === 0) {
                            for (const prod of data.products) {
                                await client.query(`
                                    INSERT INTO products (contract_id, name, brand, model, quantity)
                                    VALUES ($1, $2, $3, $4, $5)
                                `, [contractId, prod.name, prod.brand, prod.model, prod.quantity]);
                            }
                        }
                    }

                    await client.query('COMMIT');
                } catch (e) {
                    await client.query('ROLLBACK');
                    console.error('DB Insert Error:', e);
                } finally {
                    client.release();
                }
            }

            processedCount++;
            if (processedCount % 10 === 0) console.log(`[DEBUG] Processed ${processedCount}/${pdfFiles.length} files...`);

        } catch (err) {
            console.error(`[ERROR] Failed to process ${path.basename(file)}:`, err.message);
        }
    }

    if (exportRaw && workbook) {
        try {
            console.log('[DEBUG] Writing Excel file to disk...');
            const dest = path.join('data_exports', 'raw_data.xlsx');
            await workbook.xlsx.writeFile(dest);
            console.log(`[SUCCESS] Excel exported to ${dest}`);
        } catch (e) {
            console.error(`[ERROR] Failed to write Excel file:`, e);
        }
    }
}
