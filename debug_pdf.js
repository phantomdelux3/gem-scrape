import fs from 'fs-extra';
import path from 'path';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const pdf = require('pdf-parse'); // Safer for CJS modules in ESM

async function analyzePdf(filePath) {
    console.log(`Analyzing: ${filePath}`);
    const dataBuffer = await fs.readFile(filePath);
    const data = await pdf(dataBuffer);
    const rawText = data.text;

    // Clean text same way as extractor
    const cleanText = rawText.replace(/[^\x00-\x7F]/g, "").replace(/\s+/g, ' ').trim();

    const outputPath = 'debug_pdf_output.txt';
    await fs.writeFile(outputPath, `RAW TEXT:\n${rawText}\n\nCLEAN TEXT:\n${cleanText}`);
    console.log(`Dumped text to ${outputPath}`);
}

// Just pick the first PDF found
async function run() {
    const dir = 'pdfs';
    if (!await fs.pathExists(dir)) return;
    const files = await fs.readdir(dir);
    const pdfFile = files.find(f => f.toLowerCase().endsWith('.pdf'));
    if (pdfFile) {
        await analyzePdf(path.join(dir, pdfFile));
    } else {
        console.log('No PDF found to analyze');
    }
}

run();
