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
    const results = await csvBatch(fs.createReadStream(csvPath), {
      batch: true,
      batchSize: BATCH_SIZES.csvBatch,
      batchExecution: async (batch) => {
        try {
          await db.insertBatch(batch);
          profiler.update(profiler.recordsProcessed + batch.length);
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
      const stream = fs.createReadStream(csvPath);
      
      stream
        .pipe(csv())
        .on('data', async (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZES.csvParser) {
            stream.pause();
            
            try {
              await db.insertBatch(batch);
              processed += batch.length;
              profiler.update(processed);
              batch = [];
              stream.resume();
            } catch (error) {
              profiler.incrementError();
              stream.destroy();
              reject(error);
            }
          }
        })
        .on('end', async () => {
          // Process remaining records
          if (batch.length > 0) {
            try {
              await db.insertBatch(batch);
              processed += batch.length;
              profiler.update(processed);
            } catch (error) {
              profiler.incrementError();
              reject(error);
              return;
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
      let processing = false;
      
      const stream = fastcsv
        .parseFile(csvPath, { headers: true })
        .on('data', async (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZES.fastCsv && !processing) {
            processing = true;
            stream.pause();
            const currentBatch = batch;
            batch = [];
            
            try {
              await db.insertBatch(currentBatch);
              processed += currentBatch.length;
              profiler.update(processed);
              processing = false;
              stream.resume();
            } catch (error) {
              profiler.incrementError();
              stream.destroy();
              reject(error);
            }
          }
        })
        .on('end', async () => {
          // Process remaining records
          if (batch.length > 0) {
            try {
              await db.insertBatch(batch);
              processed += batch.length;
              profiler.update(processed);
            } catch (error) {
              profiler.incrementError();
              reject(error);
              return;
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
      let processing = false;
      
      Papa.parse(fs.createReadStream(csvPath), {
        header: true,
        chunkSize: BATCH_SIZES.papaParse * 200, // Approximate bytes per record
        chunk: async (results, parser) => {
          if (processing) return;
          
          processing = true;
          parser.pause();
          
          try {
            const batch = results.data;
            if (batch.length > 0) {
              await db.insertBatch(batch);
              processed += batch.length;
              profiler.update(processed);
            }
            
            processing = false;
            parser.resume();
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
      let processing = false;
      const parser = parse({ columns: true });
      
      parser.on('readable', async function() {
        if (processing) return;
        
        let record;
        while ((record = parser.read()) !== null) {
          batch.push(record);
          
          if (batch.length >= BATCH_SIZES.csvParse) {
            processing = true;
            parser.pause();
            const currentBatch = batch;
            batch = [];
            
            try {
              await db.insertBatch(currentBatch);
              processed += currentBatch.length;
              profiler.update(processed);
              processing = false;
              parser.resume();
            } catch (error) {
              profiler.incrementError();
              parser.destroy();
              reject(error);
              return;
            }
          }
        }
      });
      
      parser.on('end', async () => {
        // Process remaining records
        if (batch.length > 0) {
          try {
            await db.insertBatch(batch);
            processed += batch.length;
            profiler.update(processed);
          } catch (error) {
            profiler.incrementError();
            reject(error);
            return;
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
  
  console.log(`\nMySQL Import Benchmark - ${config.records.toLocaleString()} records`);
  console.log('='.repeat(60));
  
  const dbConnections = new DatabaseConnections();
  const results = [];
  
  try {
    // Wait for MySQL to be ready
    console.log('Connecting to MySQL...');
    const mysql = await dbConnections.initMySQL();
    console.log('Connected successfully.\n');
    
    // Benchmark each parser (streaming parsers only - realistic for large datasets)
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
    colWidths: [15, 12, 15, 15, 10, 10]
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
      result.memory.peakDelta,
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