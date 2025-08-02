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
  csvBatch: 10000,
  csvParser: 10000,
  fastCsv: 10000,
  papaParse: 10000,
  csvParse: 10000
};

async function benchmarkCsvBatch(csvPath, db, totalRecords) {
  return Profiler.measure('csv-batch', totalRecords, async (profiler) => {
    const results = await csvBatch(fs.createReadStream(csvPath), {
      batch: true,
      batchSize: BATCH_SIZES.csvBatch,
      batchExecution: async (batch) => {
        try {
          db.insertMany(batch);
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
      
      const readStream = fs.createReadStream(csvPath);
      const csvStream = csv();
      
      readStream.pipe(csvStream)
        .on('data', (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZES.csvParser) {
            // Pause stream while processing batch
            csvStream.pause();
            
            try {
              db.insertMany(batch);
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
          // Process remaining records
          if (batch.length > 0) {
            try {
              db.insertMany(batch);
              processed += batch.length;
              profiler.update(processed);
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
      
      fastcsv
        .parseFile(csvPath, { headers: true })
        .on('data', (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZES.fastCsv) {
            try {
              db.insertMany(batch);
              processed += batch.length;
              profiler.update(processed);
              batch = [];
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
        })
        .on('end', () => {
          // Process remaining records
          if (batch.length > 0) {
            try {
              db.insertMany(batch);
              processed += batch.length;
              profiler.update(processed);
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
      
      Papa.parse(fs.createReadStream(csvPath), {
        header: true,
        chunkSize: BATCH_SIZES.papaParse * 100, // Approximate bytes
        chunk: (results, parser) => {
          try {
            const batch = results.data;
            if (batch.length > 0) {
              db.insertMany(batch);
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
  });
}

async function benchmarkCsvParse(csvPath, db, totalRecords) {
  return Profiler.measure('csv-parse', totalRecords, async (profiler) => {
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
              db.insertMany(batch);
              processed += batch.length;
              profiler.update(processed);
              batch = [];
            } catch (error) {
              profiler.incrementError();
              reject(error);
            }
          }
        }
      });
      
      parser.on('end', () => {
        // Process remaining records
        if (batch.length > 0) {
          try {
            db.insertMany(batch);
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
  });
}

async function benchmarkUDSV(csvPath, db, totalRecords) {
  return Profiler.measure('uDSV', totalRecords, async (profiler) => {
    try {
      const { inferSchema, initParser } = require('udsv');
      const csvContent = require('fs').readFileSync(csvPath, 'utf8');
      const schema = inferSchema(csvContent);
      const parser = initParser(schema);
      const rows = parser.stringObjs(csvContent);
      
      // Process in batches
      const batchSize = BATCH_SIZES.csvBatch;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        db.insertMany(batch);
        profiler.update(i + batch.length);
      }
      
      return { recordsProcessed: rows.length };
    } catch (error) {
      throw new Error(`uDSV not available: ${error.message}`);
    }
  });
}

async function benchmarkCsvSimpleParser(csvPath, db, totalRecords) {
  return Profiler.measure('csv-simple-parser', totalRecords, async (profiler) => {
    try {
      const parseCsv = require('csv-simple-parser').default;
      const csvContent = require('fs').readFileSync(csvPath, 'utf8');
      const rows = parseCsv(csvContent, { header: true });
      
      // Process in batches
      const batchSize = BATCH_SIZES.csvBatch;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        db.insertMany(batch);
        profiler.update(i + batch.length);
      }
      
      return { recordsProcessed: rows.length };
    } catch (error) {
      throw new Error(`csv-simple-parser not available: ${error.message}`);
    }
  });
}

async function benchmarkButCsv(csvPath, db, totalRecords) {
  return Profiler.measure('but-csv', totalRecords, async (profiler) => {
    try {
      const { parse } = await import('but-csv');
      const csvContent = require('fs').readFileSync(csvPath, 'utf8');
      const arrays = parse(csvContent);
      
      // Convert arrays to objects
      if (arrays.length === 0) return { recordsProcessed: 0 };
      const headers = arrays[0];
      const rows = [];
      for (let i = 1; i < arrays.length; i++) {
        const obj = {};
        headers.forEach((header, index) => {
          obj[header] = arrays[i][index] || '';
        });
        rows.push(obj);
      }
      
      // Process in batches
      const batchSize = BATCH_SIZES.csvBatch;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        db.insertMany(batch);
        profiler.update(i + batch.length);
      }
      
      return { recordsProcessed: rows.length };
    } catch (error) {
      throw new Error(`but-csv not available: ${error.message}`);
    }
  });
}

async function benchmarkD3Dsv(csvPath, db, totalRecords) {
  return Profiler.measure('d3-dsv', totalRecords, async (profiler) => {
    try {
      const d3 = require('d3-dsv');
      const csvContent = require('fs').readFileSync(csvPath, 'utf8');
      const rows = d3.csvParse(csvContent);
      
      // Process in batches
      const batchSize = BATCH_SIZES.csvBatch;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        db.insertMany(batch);
        profiler.update(i + batch.length);
      }
      
      return { recordsProcessed: rows.length };
    } catch (error) {
      throw new Error(`d3-dsv not available: ${error.message}`);
    }
  });
}

async function benchmarkCommaSeparatedValues(csvPath, db, totalRecords) {
  return Profiler.measure('comma-separated-values', totalRecords, async (profiler) => {
    try {
      const CSV = require('comma-separated-values');
      const csvContent = require('fs').readFileSync(csvPath, 'utf8');
      const arrays = CSV.parse(csvContent);
      
      // Convert arrays to objects
      if (arrays.length === 0) return { recordsProcessed: 0 };
      const headers = arrays[0];
      const rows = [];
      for (let i = 1; i < arrays.length; i++) {
        const obj = {};
        headers.forEach((header, index) => {
          obj[header] = arrays[i][index] || '';
        });
        rows.push(obj);
      }
      
      // Process in batches
      const batchSize = BATCH_SIZES.csvBatch;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        db.insertMany(batch);
        profiler.update(i + batch.length);
      }
      
      return { recordsProcessed: rows.length };
    } catch (error) {
      throw new Error(`comma-separated-values not available: ${error.message}`);
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
  
  console.log(`\nSQLite Import Benchmark - ${config.records.toLocaleString()} records`);
  console.log('='.repeat(60));
  
  const dbConnections = new DatabaseConnections();
  const results = [];
  
  try {
    const sqlite = await dbConnections.initSQLite();
    
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
    
    return results;
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