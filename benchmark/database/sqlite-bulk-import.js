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
  csvBatch: 1000,   // Smaller batches for better error handling
  csvParser: 1000,
  fastCsv: 1000,
  papaParse: 1000,
  csvParse: 1000
};

// SQLite bulk optimizations - use WAL mode for better concurrency and performance
function optimizeSQLiteForBulkInsert(db) {
  // WAL mode is much faster for bulk inserts than DELETE mode
  db.pragma('journal_mode = WAL');     // Write-Ahead Logging
  db.pragma('synchronous = NORMAL');   // Good balance of safety and speed  
  db.pragma('cache_size = 20000');     // Larger cache (20MB)
  db.pragma('temp_store = MEMORY');    // Temp tables in memory
  db.pragma('mmap_size = 134217728');  // 128MB memory-mapped I/O
}

function restoreSQLiteSettings(db) {
  // Keep WAL mode - it's generally better
  db.pragma('synchronous = FULL');
  db.pragma('cache_size = 2000');     // Default cache size
}

async function benchmarkCsvBatch(csvPath, db, totalRecords) {
  return Profiler.measure('csv-batch', totalRecords, async (profiler) => {
    optimizeSQLiteForBulkInsert(db.db);
    
    try {
      const results = await csvBatch(fs.createReadStream(csvPath), {
        batch: true,
        batchSize: BATCH_SIZES.csvBatch,
        batchExecution: async (batch) => {
          try {
            // Use better-sqlite3's built-in transaction for optimal performance
            const transaction = db.db.transaction((records) => {
              for (const record of records) {
                db.insert.run(record);
              }
            });
            
            transaction(batch);
            profiler.update(profiler.recordsProcessed + batch.length);
          } catch (error) {
            profiler.incrementError();
            throw error;
          }
        }
      });
      
      return results;
    } finally {
      restoreSQLiteSettings(db.db);
    }
  });
}

async function benchmarkCsvParser(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parser', totalRecords, async (profiler) => {
    optimizeSQLiteForBulkInsert(db.db);
    
    try {

      return new Promise((resolve, reject) => {
        let batch = [];
        let processed = 0;
        
        const readStream = fs.createReadStream(csvPath);
        const csvStream = csv();
        
        readStream.pipe(csvStream)
          .on('data', (row) => {
            batch.push(row);
            
            if (batch.length >= BATCH_SIZES.csvParser) {
              csvStream.pause();
              
              try {
                const transaction = db.db.transaction((records) => {
                  for (const record of records) {
                    db.insert.run(record);
                  }
                });
                transaction(batch);
                processed += batch.length;
                profiler.update(processed);
                batch = [];
                csvStream.resume();
              } catch (error) {
                profiler.incrementError();
                reject(error);
              }
            }
          })
          .on('end', () => {
            if (batch.length > 0) {
              try {
                transaction(batch);
                processed += batch.length;
                profiler.update(processed);
              } catch (error) {
                try {
                  db.db.exec('ROLLBACK');
                } catch (rollbackError) {
                  // Ignore rollback errors
                }
                profiler.incrementError();
                reject(error);
              }
            }
            resolve();
          })
          .on('error', reject);
      });
    } finally {
      restoreSQLiteSettings(db.db);
    }
  });
}

async function benchmarkFastCsv(csvPath, db, totalRecords) {
  return Profiler.measure('fast-csv', totalRecords, async (profiler) => {
    optimizeSQLiteForBulkInsert(db.db);
    
    try {

      return new Promise((resolve, reject) => {
        let batch = [];
        let processed = 0;
        
        fastcsv
          .parseFile(csvPath, { headers: true })
          .on('data', (row) => {
            batch.push(row);
            
            if (batch.length >= BATCH_SIZES.fastCsv) {
              try {
                db.db.exec('BEGIN TRANSACTION');
                for (const record of batch) {
                  db.insert.run(record);
                }
                db.db.exec('COMMIT');
                processed += batch.length;
                profiler.update(processed);
                batch = [];
              } catch (error) {
                try {
                  db.db.exec('ROLLBACK');
                } catch (rollbackError) {
                  // Ignore rollback errors
                }
                profiler.incrementError();
                reject(error);
              }
            }
          })
          .on('end', () => {
            if (batch.length > 0) {
              try {
                transaction(batch);
                processed += batch.length;
                profiler.update(processed);
              } catch (error) {
                try {
                  db.db.exec('ROLLBACK');
                } catch (rollbackError) {
                  // Ignore rollback errors
                }
                profiler.incrementError();
                reject(error);
              }
            }
            resolve();
          })
          .on('error', reject);
      });
    } finally {
      restoreSQLiteSettings(db.db);
    }
  });
}

async function benchmarkPapaParse(csvPath, db, totalRecords) {
  return Profiler.measure('papaparse', totalRecords, async (profiler) => {
    optimizeSQLiteForBulkInsert(db.db);
    
    try {

      return new Promise((resolve, reject) => {
        let processed = 0;
        
        Papa.parse(fs.createReadStream(csvPath), {
          header: true,
          chunkSize: BATCH_SIZES.papaParse * 100,
          chunk: (results, parser) => {
            try {
              const batch = results.data.filter(row => row.id); // Filter empty rows
              if (batch.length > 0) {
                transaction(batch);
                processed += batch.length;
                profiler.update(processed);
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
    } finally {
      restoreSQLiteSettings(db.db);
    }
  });
}

async function benchmarkCsvParse(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parse', totalRecords, async (profiler) => {
    optimizeSQLiteForBulkInsert(db.db);
    
    try {

      return new Promise((resolve, reject) => {
        let batch = [];
        let processed = 0;
        const parser = parse({ columns: true });
        
        parser.on('readable', function() {
          let record;
          while ((record = parser.read()) !== null) {
            batch.push(record);
            
            if (batch.length >= BATCH_SIZES.csvParse) {
              try {
                db.db.exec('BEGIN TRANSACTION');
                for (const record of batch) {
                  db.insert.run(record);
                }
                db.db.exec('COMMIT');
                processed += batch.length;
                profiler.update(processed);
                batch = [];
              } catch (error) {
                try {
                  db.db.exec('ROLLBACK');
                } catch (rollbackError) {
                  // Ignore rollback errors
                }
                profiler.incrementError();
                reject(error);
              }
            }
          }
        });
        
        parser.on('end', () => {
          if (batch.length > 0) {
            try {
              db.db.exec('BEGIN TRANSACTION');
              for (const record of batch) {
                db.insert.run(record);
              }
              db.db.exec('COMMIT');
              processed += batch.length;
              profiler.update(processed);
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
    } finally {
      restoreSQLiteSettings(db.db);
    }
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
  
  console.log(`\nSQLite Bulk Import Benchmark - ${config.records.toLocaleString()} records`);
  console.log('='.repeat(60));
  
  const dbConnections = new DatabaseConnections();
  const results = [];
  
  try {
    const sqlite = await dbConnections.initSQLite();
    
    // Benchmark each parser with SQLite optimizations
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
        const result = await parser.fn(csvPath, sqlite, config.records);
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