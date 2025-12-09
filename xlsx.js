// import xlsx from 'xlsx';
// import oracledb from 'oracledb';
// import { COLUMN_NAMES7 } from './column-names.js';

// const FILE_PATH = '7-data.xlsx';
// const BATCH_SIZE = 50000;
// const INSERT_SQL = `INSERT INTO EXPORT_TABLE7 (${COLUMN_NAMES7.join(',')}) VALUES (${COLUMN_NAMES7.map(c => ':' + c).join(',')})`;

// async function executeBatch(connection, batch) {
//     const binds = batch.map(row => {
//         const obj = {};
//         COLUMN_NAMES7.forEach((col, i) => {
//             obj[col] = row[i] !== undefined ? row[i] : null;
//         });
//         return obj;
//     });
//     await connection.executeMany(INSERT_SQL, binds, { autoCommit: false });
//     await connection.commit();
// }

// async function run() {
//     let connection;
//     try {
//         connection = await oracledb.getConnection({
//             user: 'system',
//             password: '123321',
//             connectString: 'localhost:1522/XE'
//         });

//         const workbook = xlsx.readFile(FILE_PATH);
//         const sheetName = workbook.SheetNames[0];
//         const sheet = workbook.Sheets[sheetName];
//         const jsonData = xlsx.utils.sheet_to_json(sheet, { header: COLUMN_NAMES7, defval: null });

//         let batch = [];
//         let totalInserted = 0;

//         for (let i = 0; i < jsonData.length; i++) {
//             const row = COLUMN_NAMES7.map(col => jsonData[i][col]);
//             batch.push(row);

//             if (batch.length >= BATCH_SIZE) {
//                 await executeBatch(connection, batch);
//                 totalInserted += batch.length;
//                 console.log(`Inserted: ${totalInserted} rows`);
//                 batch = [];
//             }
//         }

//         if (batch.length > 0) {
//             await executeBatch(connection, batch);
//             totalInserted += batch.length;
//         }

//         console.log(`Finished. Total rows inserted: ${totalInserted}`);

//     } catch (err) {
//         console.error(err);
//     } finally {
//         if (connection) await connection.close();
//     }
// }

// run();






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
        console.error('\n❌ Batch xatosi:', err.message);
        console.error('\nBirinchi 2 ta muammoli qator:');
        binds.slice(0, 2).forEach((row, idx) => {
            console.error(`\nQator ${idx + 1}:`);
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
            console.error(`❌ Fayl topilmadi: ${CSV_FILE_PATH}`);
            console.error('\n📝 CSV faylni yarating:');
            console.error('   1. Excel da 7-data.xlsx ni oching');
            console.error('   2. File → Save As → CSV (Comma delimited)');
            console.error('   3. "7-data.csv" deb saqlang');
            process.exit(1);
        }

        const stats = fs.statSync(CSV_FILE_PATH);
        const fileSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
        
        console.log('🔌 Oracle bazaga ulanish...');
        connection = await oracledb.getConnection({
            user: 'system',
            password: '123321',
            connectString: 'localhost:1522/XE'
        });
        console.log('✅ Muvaffaqiyatli ulandi\n');

        console.log(`📄 Fayl: ${CSV_FILE_PATH}`);
        console.log(`📊 Hajmi: ${fileSizeMB} MB`);
        console.log(`🔧 Ajratuvchi: "${DELIMITER}" (semicolon)`);
        
        console.log('\n📊 Qatorlarni sanash...');
        const totalLines = await countFileLines(CSV_FILE_PATH);
        console.log(`📄 Jami qatorlar: ${(totalLines - 1).toLocaleString()} (header dan tashqari)`);
        console.log(`📦 Batch hajmi: ${BATCH_SIZE.toLocaleString()}\n`);
        console.log('='.repeat(70));

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
                console.log(`📋 Header topildi: ${headerColumns} ta ustun`);
                console.log(`📋 Kutilayotgan ustunlar: ${COLUMN_NAMES7.length}`);
                
                if (headerColumns !== COLUMN_NAMES7.length) {
                    console.warn(`\n⚠️  OGOHLANTIRISH: Ustunlar soni mos kelmaydi!`);
                    console.warn(`   Faylda: ${headerColumns}, Kutilgan: ${COLUMN_NAMES7.length}`);
                    console.warn(`   Header ustunlari:`);
                    headerValues.forEach((col, idx) => {
                        console.warn(`      ${idx + 1}. ${col}`);
                    });
                }
                
                console.log('');
                isFirstLine = false;
                continue;
            }

            if (!line.trim()) {
                continue;
            }

            const values = parseCSVLine(line);
            
            if (values.length !== COLUMN_NAMES7.length) {
                skippedLines++;
                if (skippedLines <= 3) {
                    console.warn(`⚠️  Qator ${lineNumber}: ${COLUMN_NAMES7.length} ustun kutilgan, ${values.length} topildi`);
                    if (skippedLines === 1) {
                        console.warn(`   Ma'lumot: ${line.substring(0, 100)}...`);
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
                    `📊 ${percent}% | ` +
                    `Yuklandi: ${totalInserted.toLocaleString()} | ` +
                    `Tezlik: ${rowsPerSec.toLocaleString()} qator/sek | ` +
                    `Qoldi: ~${etaMinutes}m`
                );
                
                batch = [];
            }

            if (Date.now() - lastUpdate > 5000 && batch.length > 0) {
                const percent = ((lineNumber / totalLines) * 100).toFixed(1);
                console.log(`📖 O'qilmoqda: ${percent}% | Buffer: ${batch.length.toLocaleString()} qator`);
                lastUpdate = Date.now();
            }
        }

        if (batch.length > 0) {
            console.log(`\n📤 Oxirgi batch yuklanmoqda: ${batch.length.toLocaleString()} qator...`);
            await executeBatch(connection, batch);
            totalInserted += batch.length;
        }

        const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
        const avgSpeed = Math.round(totalInserted / totalTime);

        console.log('\n' + '='.repeat(70));
        console.log('✅ MUVAFFAQIYATLI YAKUNLANDI!');
        console.log(`📊 Jami yuklandi: ${totalInserted.toLocaleString()} qator`);
        if (skippedLines > 0) {
            console.log(`⚠️  O'tkazib yuborildi: ${skippedLines.toLocaleString()} qator`);
        }
        console.log(`⏱️  Vaqt: ${totalTime}s (${(totalTime / 60).toFixed(1)} daqiqa)`);
        console.log(`⚡ O'rtacha tezlik: ${avgSpeed.toLocaleString()} qator/sek`);
        console.log('='.repeat(70));

    } catch (err) {
        console.error('\n❌ KRITIK XATO:', err.message);
        console.error('Stack:', err.stack);
    } finally {
        if (connection) {
            try {
                await connection.close();
                console.log('\n Baza aloqasi yopildi');
            } catch (e) {
                console.error('Aloqa yopishda xato:', e.message);
            }
        }
    }
}

run().catch(err => {
    console.error('Umumiy xato:', err);
    process.exit(1);
});