import { promises as fsp } from 'fs';  
import fs from 'fs';                    

import readline from 'readline';
import oracledb from 'oracledb';
import { COLUMN_NAMES6 } from './column-names.js';


const FILE_PATH = '6-data.excel';
const BATCH_SIZE = 50000;


const INSERT_SQL = `INSERT INTO EXPORT_TABLE6 (${COLUMN_NAMES6.join(',')}) VALUES (${COLUMN_NAMES6.map(c => ':' + c).join(',')})`;
const NUMERIC_COLUMN_START_INDEX = 6;

const DATE_REGEX = /to_date\('([^']*)',/i;

function parseValuesOptimized(valuesStr) {
    const values = [];
    let currentPos = 0;
    let columnIndex = 0;

    while (currentPos < valuesStr.length) {
        let valueEnd = -1;
        let inQuotes = false;
        let depth = 0;

        for (let i = currentPos; i < valuesStr.length; i++) {
            const char = valuesStr[i];

            if (char === "'") {
                if (i + 1 < valuesStr.length && valuesStr[i + 1] === "'") {
                    i++;
                    continue;
                }
                inQuotes = !inQuotes;
            } else if (!inQuotes && char === '(') {
                depth++;
            } else if (!inQuotes && char === ')') {
                depth--;
            } else if (!inQuotes && depth === 0 && char === ',') {
                valueEnd = i;
                break;
            }
        }

        if (valueEnd === -1) {
            valueEnd = valuesStr.length;
        }

        const rawValue = valuesStr.substring(currentPos, valueEnd);
        let processedValue;
        const trimmedValue = rawValue.trim();

        if (trimmedValue.toUpperCase() === 'NULL' || trimmedValue.length === 0) {
            processedValue = null;
        }
        else if (trimmedValue.startsWith("'") && trimmedValue.endsWith("'")) {
            processedValue = trimmedValue.slice(1, -1).replace(/''/g, "'");
        }
        else if (trimmedValue.toUpperCase().startsWith('TO_DATE(')) {
            const match = trimmedValue.match(DATE_REGEX);
            if (match && match[1]) {
                processedValue = new Date(match[1]);
            } else {
                processedValue = trimmedValue;
            }
        }
        else {
            const num = Number(trimmedValue);
            if (columnIndex >= NUMERIC_COLUMN_START_INDEX) {
                processedValue = isNaN(num) || !isFinite(num) ? null : num;
            } else {
                processedValue = isNaN(num) || !isFinite(num) ? trimmedValue : num;
            }
        }

        values.push(processedValue);
        currentPos = valueEnd + 1;
        columnIndex++;
    }

    return values;
}

async function executeBatch(connection, batch) {
    const validBatch = batch.filter(row => {
        if (!Array.isArray(row)) {
            console.warn("Skipping non-array row");
            return false;
        }
        if (row.length !== COLUMN_NAMES6.length) {
            console.warn(`Skipping row with ${row.length} columns (expected ${COLUMN_NAMES6.length})`);
            return false;
        }
        return true;
    });

    if (validBatch.length === 0) {
        console.warn("No valid rows in batch, skipping");
        return;
    }

    const binds = validBatch.map(row => {
        const obj = {};
        COLUMN_NAMES6.forEach((col, i) => {
            obj[col] = row[i] !== undefined ? row[i] : null;
        });
        return obj;
    });

    try {
        await connection.executeMany(INSERT_SQL, binds, {
            autoCommit: false
        });
        await connection.commit();
    } catch (err) {
        console.error("Batch insertion failed:", err.message);
        console.error("Error code:", err.code || 'N/A');

        console.error("DEBUG - First row structure:");
        const firstBind = binds[0];
        console.error("Keys:", Object.keys(firstBind).length);
        console.error("Expected columns:", COLUMN_NAMES6.length);

        console.error("First 10 columns of first row:");
        Object.entries(firstBind).slice(0, 10).forEach(([key, val]) => {
            console.error(`${key}: ${val} (${typeof val})`);
        });

        throw err;
    }
}

function countFileLines(path) {
    return new Promise((resolve) => {
        let lineCount = 0;
        const rl = readline.createInterface({
            input: fs.createReadStream(path, { encoding: 'utf8' }),
            crlfDelay: Infinity
        });

        rl.on('line', () => lineCount++);
        rl.on('close', () => resolve(lineCount));
    });
}

async function run() {
    let connection;
    let rl;
    const startTime = Date.now();

    try {
        oracledb.maxRows = 0;
        oracledb.fetchAsString = [oracledb.DATE];

        console.log("Connecting to Oracle database...");
        connection = await oracledb.getConnection({
            user: 'system',
            password: '123321',
            connectString: 'localhost:1522/XE'
        });
        console.log("Connected successfully");

        const totalLines = await countFileLines(FILE_PATH);
        console.log(`Total lines in file: ${totalLines}`);
        console.log(`Batch size: ${BATCH_SIZE}`);
        console.log(`Started at: ${new Date().toLocaleTimeString()}`);
        console.log("----------------------------------------------------------------------");

        rl = readline.createInterface({
            input: fs.createReadStream(FILE_PATH, { encoding: 'utf8' }),
            crlfDelay: Infinity
        });

        let buffer = '';
        let batch = [];
        let totalExecuted = 0;
        let processedLines = 0;
        let skippedLines = 0;
        let parsedStatements = 0;
        let lastProgressUpdate = Date.now();

        for await (const line of rl) {
            processedLines++;
            const trimmed = line.trim();

            if (!trimmed || trimmed.startsWith('REM') || trimmed.startsWith('SET')) {
                continue;
            }

            buffer += trimmed + ' ';

            if (trimmed.endsWith(';')) {
                const valuesMatch = buffer.match(/values\s*\((.+)\);?\s*$/i);

                if (valuesMatch && valuesMatch[1]) {
                    parsedStatements++;
                    const valuesStr = valuesMatch[1];
                    const values = parseValuesOptimized(valuesStr);

                    if (parsedStatements <= 3) {
                        console.log(`DEBUG - Statement ${parsedStatements}:`);
                        console.log(`   Parsed ${values.length} values (expected ${COLUMN_NAMES6.length})`);
                        console.log(`   First 5 values:`, values.slice(0, 5));
                    }

                    if (values.length === COLUMN_NAMES6.length) {
                        batch.push(values);
                    } else {
                        skippedLines++;
                        if (skippedLines <= 5) {
                            console.warn(
                                `Line ${processedLines}: Expected ${COLUMN_NAMES6.length} values, got ${values.length}`
                            );
                            console.warn(`Statement preview: ${buffer.substring(0, 100)}...`);
                        }
                    }
                } else if (buffer.toLowerCase().includes('insert into')) {
                    skippedLines++;
                    if (skippedLines <= 3) {
                        console.warn(`Line ${processedLines}: Could not parse INSERT statement`);
                        console.warn(`Statement: ${buffer.substring(0, 150)}...`);
                    }
                }

                buffer = '';
            }

            if (batch.length >= BATCH_SIZE) {
                console.log(`Executing batch of ${batch.length} rows...`);
                await executeBatch(connection, batch);
                totalExecuted += batch.length;

                const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                const rowsPerSec = Math.round(totalExecuted / elapsed);
                const percent = ((processedLines / totalLines) * 100).toFixed(1);
                const remaining = totalLines - processedLines;
                const etaSeconds = remaining / (rowsPerSec || 1);
                const etaMinutes = Math.round(etaSeconds / 60);

                console.log(
                    `Progress: ${percent}% | Inserted: ${totalExecuted} rows | Speed: ${rowsPerSec} rows/sec | ETA: ~${etaMinutes}m`
                );

                batch = [];
            }

            if (Date.now() - lastProgressUpdate > 5000 && batch.length > 0) {
                const percent = ((processedLines / totalLines) * 100).toFixed(1);
                console.log(`Reading: ${percent}% | Parsed: ${parsedStatements} | Buffered: ${batch.length}`);
                lastProgressUpdate = Date.now();
            }
        }

        if (batch.length > 0) {
            console.log(`Inserting final batch of ${batch.length} rows...`);
            await executeBatch(connection, batch);
            totalExecuted += batch.length;
        }

        const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
        const avgSpeed = Math.round(totalExecuted / totalTime);

        console.log("----------------------------------------------------------------------");
        console.log("Finished");
        console.log(`Total rows inserted: ${totalExecuted}`);
        console.log(`Total statements parsed: ${parsedStatements}`);
        console.log(`Total time: ${totalTime}s (${(totalTime / 60).toFixed(1)} minutes)`);
        console.log(`Average speed: ${avgSpeed} rows/sec`);
        if (skippedLines > 0) {
            console.log(`Skipped lines: ${skippedLines}`);
        }
        console.log("----------------------------------------------------------------------");

    } catch (err) {
        console.error("FATAL ERROR:", err.message);
        console.error("Stack:", err.stack);
    } finally {
        if (rl) rl.close();
        if (connection) {
            try {
                await connection.close();
                console.log("Database connection closed");
            } catch (e) {
                console.error("Error closing connection:", e.message);
            }
        }
    }
}

run().catch(err => {
    console.error("Unhandled error:", err);
    process.exit(1);
});

