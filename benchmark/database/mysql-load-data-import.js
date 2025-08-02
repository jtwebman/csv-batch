'use strict';

const fs = require('fs');
const path = require('path');
const csvBatch = require('../../index');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');
const Papa = require('papaparse');
const { parse } = require('csv-parse');
const DatabaseConnections = require('../lib/db-connections');
const Profiler = require('../lib/profiler');

const BATCH_SIZES = {
  csvBatch: 5000,
  csvParser: 5000,
  fastCsv: 5000,
  papaParse: 5000,
  csvParse: 5000
};

// Helper to write temp CSV file for LOAD DATA INFILE
async function writeTempCsvFile(records, tempPath) {
  const writeStream = fs.createWriteStream(tempPath);
  
  for (const record of records) {
    // Escape any quotes and write CSV line
    const csvLine = [
      record.id,
      `"${record.name.replace(/"/g, '""')}"`,
      `"${record.email.replace(/"/g, '""')}"`,
      record.age,
      record.salary,
      `"${record.department.replace(/"/g, '""')}"`,
      `"${record.created_at.replace(/"/g, '""')}"`
    ].join(',') + '\n';
    
    writeStream.write(csvLine);
  }
  
  return new Promise((resolve, reject) => {
    writeStream.end();
    writeStream.on('finish', resolve);
    writeStream.on('error', reject);
  });
}

async function loadDataInfile(db, tempPath, recordCount, profiler) {
  // MySQL LOAD DATA INFILE - much faster than INSERT
  const query = `
    LOAD DATA INFILE '${tempPath}'
    INTO TABLE users
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    (id, name, email, age, salary, department, created_at)
  `;
  
  await db.pool.execute(query);
  profiler.update(recordCount);
  
  // Clean up temp file
  fs.unlinkSync(tempPath);
}

async function benchmarkCsvBatch(csvPath, db, totalRecords) {
  return Profiler.measure('csv-batch', totalRecords, async (profiler) => {
    let batchCount = 0;
    
    const results = await csvBatch(fs.createReadStream(csvPath), {
      batch: true,
      batchSize: BATCH_SIZES.csvBatch,
      batchExecution: async (batch) => {
        try {
          // Write batch to temp file
          const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
          await writeTempCsvFile(batch, tempPath);
          
          // Use LOAD DATA INFILE
          await loadDataInfile(db, tempPath, batch.length, profiler);
        } catch (error) {
          profiler.incrementError();
          throw error;
        }
      }
    });
    
    return results;
  });
}

async function benchmarkCsvParser(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parser', totalRecords, async (profiler) => {
    return new Promise((resolve, reject) => {
      let batch = [];
      let processed = 0;
      let batchCount = 0;
      const stream = fs.createReadStream(csvPath);
      
      stream
        .pipe(csv())
        .on('data', async (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZES.csvParser) {
            stream.pause();
            
            try {
              // Write batch to temp file
              const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
              await writeTempCsvFile(batch, tempPath);
              
              // Use LOAD DATA INFILE
              const query = `
                LOAD DATA INFILE '${tempPath}'
                INTO TABLE users
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                (id, name, email, age, salary, department, created_at)
              `;
              
              await db.pool.execute(query);
              processed += batch.length;
              profiler.update(processed);
              
              // Clean up
              fs.unlinkSync(tempPath);
              batch = [];
              stream.resume();
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
        })
        .on('end', async () => {
          // Process remaining records
          if (batch.length > 0) {
            try {
              const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
              await writeTempCsvFile(batch, tempPath);
              
              const query = `
                LOAD DATA INFILE '${tempPath}'
                INTO TABLE users
                FIELDS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                LINES TERMINATED BY '\\n'
                (id, name, email, age, salary, department, created_at)
              `;
              
              await db.pool.execute(query);
              processed += batch.length;
              profiler.update(processed);
              fs.unlinkSync(tempPath);
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
          resolve();
        })
        .on('error', reject);
    });
  });
}

async function benchmarkFastCsv(csvPath, db, totalRecords) {
  return Profiler.measure('fast-csv', totalRecords, async (profiler) => {
    return new Promise((resolve, reject) => {
      let batch = [];
      let processed = 0;
      let batchCount = 0;
      
      fastcsv
        .parseFile(csvPath, { headers: true })
        .on('data', async (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZES.fastCsv) {
            try {
              const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
              await writeTempCsvFile(batch, tempPath);
              await loadDataInfile(db, tempPath, batch.length, profiler);
              processed += batch.length;
              batch = [];
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
        })
        .on('end', async () => {
          if (batch.length > 0) {
            try {
              const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
              await writeTempCsvFile(batch, tempPath);
              await loadDataInfile(db, tempPath, batch.length, profiler);
              processed += batch.length;
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
          resolve();
        })
        .on('error', reject);
    });
  });
}

async function benchmarkPapaParse(csvPath, db, totalRecords) {
  return Profiler.measure('papaparse', totalRecords, async (profiler) => {
    return new Promise((resolve, reject) => {
      let processed = 0;
      let batchCount = 0;
      
      Papa.parse(fs.createReadStream(csvPath), {
        header: true,
        chunkSize: BATCH_SIZES.papaParse * 100,
        chunk: async (results, parser) => {
          try {
            const batch = results.data.filter(row => row.id);
            if (batch.length > 0) {
              const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
              await writeTempCsvFile(batch, tempPath);
              await loadDataInfile(db, tempPath, batch.length, profiler);
              processed += batch.length;
            }
          } catch (error) {
            profiler.incrementError();
            parser.abort();
            reject(error);
          }
        },
        complete: () => resolve(),
        error: (error) => reject(error)
      });
    });
  });
}

async function benchmarkCsvParse(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parse', totalRecords, async (profiler) => {
    return new Promise((resolve, reject) => {
      let batch = [];
      let processed = 0;
      let batchCount = 0;
      const parser = parse({ columns: true });
      
      parser.on('readable', async function() {
        let record;
        while ((record = parser.read()) !== null) {
          batch.push(record);
          
          if (batch.length >= BATCH_SIZES.csvParse) {
            parser.pause();
            
            try {
              const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
              await writeTempCsvFile(batch, tempPath);
              await loadDataInfile(db, tempPath, batch.length, profiler);
              processed += batch.length;
              batch = [];
              parser.resume();
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
        }
      });
      
      parser.on('end', async () => {
        if (batch.length > 0) {
          try {
            const tempPath = `/tmp/mysql_batch_${batchCount++}.csv`;
            await writeTempCsvFile(batch, tempPath);
            await loadDataInfile(db, tempPath, batch.length, profiler);
            processed += batch.length;
          } catch (error) {
            profiler.incrementError();
            reject(error);
          }
        }
        resolve();
      });
      
      parser.on('error', reject);
      
      fs.createReadStream(csvPath).pipe(parser);
    });
  });
}

async function runBenchmarks(size = 'medium') {
  const { fileSizes } = require('../data/generate-data');
  const config = fileSizes[size];
  const csvPath = path.join(__dirname, '..', 'results', config.file);
  
  // Check if data file exists
  if (!fs.existsSync(csvPath)) {
    console.error(`Data file not found: ${csvPath}`);
    console.error(`Run: node benchmark/data/generate-data.js ${size}`);
    process.exit(1);
  }
  
  console.log(`\nMySQL LOAD DATA Import Benchmark - ${config.records.toLocaleString()} records`);
  console.log('='.repeat(60));
  
  const dbConnections = new DatabaseConnections();
  const results = [];
  
  try {
    console.log('Connecting to MySQL...');
    const mysql = await dbConnections.initMySQL();
    console.log('Connected successfully.\n');
    
    // Benchmark each parser using LOAD DATA INFILE (streaming parsers only - realistic for large datasets)
    const parsers = [
      { name: 'csv-batch', fn: benchmarkCsvBatch },
      { name: 'csv-parser', fn: benchmarkCsvParser },
      { name: 'fast-csv', fn: benchmarkFastCsv },
      { name: 'papaparse', fn: benchmarkPapaParse },
      { name: 'csv-parse', fn: benchmarkCsvParse }
    ];
    
    for (const parser of parsers) {
      console.log(`\nRunning ${parser.name}...`);
      await dbConnections.truncateAll();
      
      try {
        const result = await parser.fn(csvPath, mysql, config.records);
        results.push(result);
      } catch (error) {
        console.error(`Error in ${parser.name}:`, error.message);
        results.push({
          name: parser.name,
          error: error.message
        });
      }
    }
    
    // Print comparison table
    printComparisonTable(results);
    
  } catch (error) {
    console.error('Failed to connect to MySQL:', error.message);
    console.error('Make sure MySQL is running: cd benchmark && docker-compose up -d');
  } finally {
    await dbConnections.closeAll();
  }
}

function printComparisonTable(results) {
  const Table = require('cli-table3');
  const table = new Table({
    head: ['Parser', 'Time', 'Records/sec', 'Memory Delta', 'GC Count', 'Status'],
    colWidths: [18, 12, 15, 15, 10, 10]
  });
  
  const validResults = results.filter(r => !r.error);
  if (validResults.length === 0) {
    console.log('\nNo successful results to compare.');
    return;
  }
  
  const fastest = validResults.reduce((min, r) => 
    r.duration.ms < min.duration.ms ? r : min, validResults[0]);
  
  for (const result of results) {
    if (result.error) {
      table.push([result.name, '-', '-', '-', '-', 'ERROR']);
      continue;
    }
    
    const timeRatio = result === fastest ? '' : ` (${(result.duration.ms / fastest.duration.ms).toFixed(2)}x)`;
    
    table.push([
      result.name,
      result.duration.formatted + timeRatio,
      result.throughput.recordsPerSecond.toLocaleString(),
      result.memory.heap.formatted.peakDelta,
      result.gcCount,
      '✓'
    ]);
  }
  
  console.log('\n' + table.toString());
}

if (require.main === module) {
  const size = process.argv[2] || 'medium';
  runBenchmarks(size).catch(console.error);
}

module.exports = { runBenchmarks };