import fs from 'fs';
import readline from 'readline';
import oracledb from 'oracledb';
import { COLUMN_NAMES7 } from './column-names.js';

const CSV_FILE_PATH = '7-datas.csv';
const BATCH_SIZE = 50000;
const DELIMITER = ';';
const INSERT_SQL = `INSERT INTO EXPORT_TABLE7 (${COLUMN_NAMES7.join(',')}) VALUES (${COLUMN_NAMES7.map(c => ':' + c).join(',')})`;

function parseCSVLine(line, delimiter = DELIMITER) {
    const values = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"') {
            if (inQuotes && line[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (char === delimiter && !inQuotes) {
            values.push(current.trim() === '' ? null : current.trim());
            current = '';
        } else {
            current += char;
        }
    }
    
    values.push(current.trim() === '' ? null : current.trim());
    return values;
}

function parseValue(value, columnName) {
    if (value === null || value === '' || value === 'NULL') {
        return null;
    }
    
    if (columnName.includes('TIME') || columnName.includes('DATE')) {
        if (value.includes('.')) {
            const parts = value.split(' ');
            const datePart = parts[0];
            const [day, month, year] = datePart.split('.');
            
            let dateStr = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
            if (parts[1]) {
                dateStr += ' ' + parts[1];
            } else {
                dateStr += ' 00:00:00';
            }
            
            return new Date(dateStr);
        }
    }
    
    const num = parseFloat(value);
    if (!isNaN(num)) {
        return num;
    }
    
    return value;
}

async function executeBatch(connection, batch) {
    if (batch.length === 0) return;
    
    const binds = batch.map(row => {
        const obj = {};
        COLUMN_NAMES7.forEach((col, i) => {
            obj[col] = parseValue(row[i], col);
        });
        return obj;
    });

    try {
        await connection.executeMany(INSERT_SQL, binds, { autoCommit: false });
        await connection.commit();
    } catch (err) {
        console.error('Batch error:', err.message);
        console.error('First 2 problematic rows:');
        binds.slice(0, 2).forEach((row, idx) => {
            console.error(`Row ${idx + 1}:`);
            Object.entries(row).slice(0, 8).forEach(([key, val]) => {
                console.error(`  ${key}: ${val} (${typeof val})`);
            });
        });
        throw err;
    }
}

function countFileLines(path) {
    return new Promise((resolve) => {
        let count = 0;
        const rl = readline.createInterface({
            input: fs.createReadStream(path),
            crlfDelay: Infinity
        });
        rl.on('line', () => count++);
        rl.on('close', () => resolve(count));
    });
}

async function run() {
    let connection;
    const startTime = Date.now();

    try {
        if (!fs.existsSync(CSV_FILE_PATH)) {
            console.error(`File not found: ${CSV_FILE_PATH}`);
            console.error('Create the CSV file:');
            console.error('1. Open 7-data.xlsx in Excel');
            console.error('2. Save As → CSV');
            console.error('3. Save it as "7-data.csv"');
            process.exit(1);
        }

        const stats = fs.statSync(CSV_FILE_PATH);
        const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
        
        console.log('Connecting to Oracle database...');
        connection = await oracledb.getConnection({
            user: 'system',
            password: '123321',
            connectString: 'localhost:1522/XE'
        });
        console.log('Connected successfully');

        console.log(`File: ${CSV_FILE_PATH}`);
        console.log(`Size: ${fileSizeMB} MB`);
        console.log(`Delimiter: "${DELIMITER}"`);

        console.log('Counting lines...');
        const totalLines = await countFileLines(CSV_FILE_PATH);
        console.log(`Total rows: ${(totalLines - 1).toLocaleString()} (excluding header)`);
        console.log(`Batch size: ${BATCH_SIZE.toLocaleString()}`);
        console.log('----------------------------------------------------------------------');

        const rl = readline.createInterface({
            input: fs.createReadStream(CSV_FILE_PATH, { encoding: 'utf8' }),
            crlfDelay: Infinity
        });

        let batch = [];
        let totalInserted = 0;
        let lineNumber = 0;
        let skippedLines = 0;
        let isFirstLine = true;
        let lastUpdate = Date.now();
        let headerColumns = 0;

        for await (const line of rl) {
            lineNumber++;
            
            if (isFirstLine) {
                const headerValues = parseCSVLine(line);
                headerColumns = headerValues.length;
                console.log(`Header detected: ${headerColumns} columns`);
                console.log(`Expected columns: ${COLUMN_NAMES7.length}`);
                
                if (headerColumns !== COLUMN_NAMES7.length) {
                    console.warn('WARNING: Column count does not match');
                    console.warn(`In file: ${headerColumns}, Expected: ${COLUMN_NAMES7.length}`);
                    console.warn('Header columns:');
                    headerValues.forEach((col, idx) => {
                        console.warn(`${idx + 1}. ${col}`);
                    });
                }
                
                console.log('');
                isFirstLine = false;
                continue;
            }

            if (!line.trim()) continue;

            const values = parseCSVLine(line);
            
            if (values.length !== COLUMN_NAMES7.length) {
                skippedLines++;
                if (skippedLines <= 3) {
                    console.warn(`Row ${lineNumber}: Expected ${COLUMN_NAMES7.length} columns, found ${values.length}`);
                    if (skippedLines === 1) {
                        console.warn(`Row data: ${line.substring(0, 100)}...`);
                    }
                }
                continue;
            }

            batch.push(values);

            if (batch.length >= BATCH_SIZE) {
                await executeBatch(connection, batch);
                totalInserted += batch.length;
                
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                const rowsPerSec = Math.round(totalInserted / elapsed);
                const percent = ((lineNumber / totalLines) * 100).toFixed(1);
                const remaining = totalLines - lineNumber;
                const etaSeconds = remaining / (rowsPerSec || 1);
                const etaMinutes = Math.round(etaSeconds / 60);
                
                console.log(
                    `${percent}% | Inserted: ${totalInserted.toLocaleString()} | Speed: ${rowsPerSec.toLocaleString()} rows/sec | ETA: ~${etaMinutes} minutes`
                );
                
                batch = [];
            }

            if (Date.now() - lastUpdate > 5000 && batch.length > 0) {
                const percent = ((lineNumber / totalLines) * 100).toFixed(1);
                console.log(`Reading: ${percent}% | Buffer: ${batch.length.toLocaleString()} rows`);
                lastUpdate = Date.now();
            }
        }

        if (batch.length > 0) {
            console.log(`Loading final batch: ${batch.length.toLocaleString()} rows...`);
            await executeBatch(connection, batch);
            totalInserted += batch.length;
        }

        const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
        const avgSpeed = Math.round(totalInserted / totalTime);

        console.log('----------------------------------------------------------------------');
        console.log('PROCESS COMPLETED');
        console.log(`Total inserted: ${totalInserted.toLocaleString()} rows`);
        if (skippedLines > 0) {
            console.log(`Skipped: ${skippedLines.toLocaleString()} rows`);
        }
        console.log(`Time: ${totalTime}s (${(totalTime / 60).toFixed(1)} minutes)`);
        console.log(`Average speed: ${avgSpeed.toLocaleString()} rows/sec`);
        console.log('----------------------------------------------------------------------');

    } catch (err) {
        console.error('CRITICAL ERROR:', err.message);
        console.error('Stack:', err.stack);
    } finally {
        if (connection) {
            try {
                await connection.close();
                console.log('Database connection closed');
            } catch (e) {
                console.error('Error while closing connection:', e.message);
            }
        }
    }
}

run().catch(err => {
    console.error('Unhandled error:', err);
    process.exit(1);
});
