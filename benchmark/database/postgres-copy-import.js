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

async function benchmarkCsvBatch(csvPath, db, totalRecords) {
  return Profiler.measure('csv-batch', totalRecords, async (profiler) => {
    const copyStream = await db.copyStream();
    let processed = 0;
    
    const results = await csvBatch(fs.createReadStream(csvPath), {
      header: false, // COPY expects raw CSV
      batch: true,
      batchSize: 10000,
      batchExecution: async (batch) => {
        try {
          for (const record of batch) {
            const line = `${record.join(',')}\n`;
            copyStream.write(line);
          }
          processed += batch.length;
          profiler.update(processed);
        } catch (error) {
          profiler.incrementError();
          throw error;
        }
      }
    });
    
    return new Promise((resolve, reject) => {
      copyStream.end();
      copyStream.on('finish', resolve);
      copyStream.on('error', reject);
    });
  });
}

async function benchmarkCsvParser(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parser', totalRecords, async (profiler) => {
    const copyStream = await db.copyStream();
    let processed = 0;
    
    return new Promise((resolve, reject) => {
      const stream = fs.createReadStream(csvPath);
      
      stream
        .pipe(csv())
        .on('data', (row) => {
          try {
            // Convert object to CSV line
            const line = `${row.id},"${row.name}","${row.email}",${row.age},${row.salary},"${row.department}","${row.created_at}"\n`;
            copyStream.write(line);
            processed++;
            if (processed % 5000 === 0) {
              profiler.update(processed);
            }
          } catch (error) {
            profiler.incrementError();
            reject(error);
          }
        })
        .on('end', () => {
          profiler.update(processed);
          copyStream.end();
          copyStream.on('finish', resolve);
          copyStream.on('error', reject);
        })
        .on('error', reject);
    });
  });
}

async function benchmarkFastCsv(csvPath, db, totalRecords) {
  return Profiler.measure('fast-csv', totalRecords, async (profiler) => {
    const copyStream = await db.copyStream();
    let processed = 0;
    
    return new Promise((resolve, reject) => {
      fastcsv
        .parseFile(csvPath, { headers: true })
        .on('data', (row) => {
          try {
            // Convert object to CSV line
            const line = `${row.id},"${row.name}","${row.email}",${row.age},${row.salary},"${row.department}","${row.created_at}"\n`;
            copyStream.write(line);
            processed++;
            if (processed % 5000 === 0) {
              profiler.update(processed);
            }
          } catch (error) {
            profiler.incrementError();
            reject(error);
          }
        })
        .on('end', () => {
          profiler.update(processed);
          copyStream.end();
          copyStream.on('finish', resolve);
          copyStream.on('error', reject);
        })
        .on('error', reject);
    });
  });
}

async function benchmarkPapaParse(csvPath, db, totalRecords) {
  return Profiler.measure('papaparse', totalRecords, async (profiler) => {
    const copyStream = await db.copyStream();
    let processed = 0;
    
    return new Promise((resolve, reject) => {
      Papa.parse(fs.createReadStream(csvPath), {
        header: true,
        chunkSize: BATCH_SIZES.papaParse * 100,
        chunk: (results, parser) => {
          try {
            for (const row of results.data) {
              if (row.id) {  // Skip empty rows
                const line = `${row.id},"${row.name}","${row.email}",${row.age},${row.salary},"${row.department}","${row.created_at}"\n`;
                copyStream.write(line);
                processed++;
              }
            }
            if (processed % 5000 === 0) {
              profiler.update(processed);
            }
          } catch (error) {
            profiler.incrementError();
            parser.abort();
            reject(error);
          }
        },
        complete: () => {
          profiler.update(processed);
          copyStream.end();
          copyStream.on('finish', resolve);
          copyStream.on('error', reject);
        },
        error: (error) => reject(error)
      });
    });
  });
}

async function benchmarkCsvParse(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parse', totalRecords, async (profiler) => {
    const copyStream = await db.copyStream();
    let processed = 0;
    
    return new Promise((resolve, reject) => {
      const parser = parse({ columns: true });
      
      parser.on('readable', function() {
        let record;
        while ((record = parser.read()) !== null) {
          try {
            const line = `${record.id},"${record.name}","${record.email}",${record.age},${record.salary},"${record.department}","${record.created_at}"\n`;
            copyStream.write(line);
            processed++;
            if (processed % 5000 === 0) {
              profiler.update(processed);
            }
          } catch (error) {
            profiler.incrementError();
            reject(error);
          }
        }
      });
      
      parser.on('end', () => {
        profiler.update(processed);
        copyStream.end();
        copyStream.on('finish', resolve);
        copyStream.on('error', reject);
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
  
  console.log(`\nPostgreSQL COPY Import Benchmark - ${config.records.toLocaleString()} records`);
  console.log('='.repeat(60));
  
  const dbConnections = new DatabaseConnections();
  const results = [];
  
  try {
    console.log('Connecting to PostgreSQL...');
    const postgres = await dbConnections.initPostgreSQL();
    console.log('Connected successfully.\n');
    
    // Benchmark each parser using COPY (streaming parsers only - realistic for large datasets)
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
        const result = await parser.fn(csvPath, postgres, config.records);
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
    console.error('Failed to connect to PostgreSQL:', error.message);
    console.error('Make sure PostgreSQL is running: cd benchmark && docker-compose up -d');
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