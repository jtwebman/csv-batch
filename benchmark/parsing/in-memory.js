'use strict';

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

// Import all CSV parsers for comparison
const csvBatch = require('../../index');
const csv = require('csv-parser');
const fastcsv = require('fast-csv');
const Papa = require('papaparse');
const { parse } = require('csv-parse');

// Helper to create readable stream from string (for parsers that need streams)
const { Readable } = require('stream');
function createStreamFromString(str) {
  return new Readable({
    read() {
      this.push(str);
      this.push(null);
    }
  });
}

// Memory tracking utility
function getMemoryUsage() {
  return process.memoryUsage();
}

// Performance measurement utility
class PerfMeasure {
  constructor(name) {
    this.name = name;
    this.times = [];
    this.memory = { start: null, peak: 0, end: null };
  }

  start() {
    global.gc && global.gc(); // Force GC if available
    this.memory.start = getMemoryUsage();
    this.memory.peak = this.memory.start.heapUsed;
    this.startTime = performance.now();
    
    // Monitor memory during execution
    this.memoryInterval = setInterval(() => {
      const current = getMemoryUsage().heapUsed;
      if (current > this.memory.peak) {
        this.memory.peak = current;
      }
    }, 1);
  }

  end() {
    const endTime = performance.now();
    this.times.push(endTime - this.startTime);
    clearInterval(this.memoryInterval);
    this.memory.end = getMemoryUsage();
    return this.times[this.times.length - 1];
  }

  getStats(csvContent, rowCount) {
    const avgTime = this.times.reduce((a, b) => a + b, 0) / this.times.length;
    const csvSizeMB = Buffer.byteLength(csvContent, 'utf8') / 1024 / 1024;
    
    return {
      name: this.name,
      avgTime,
      rowsPerSec: Math.round((rowCount / avgTime) * 1000),
      mbPerSec: Number(((csvSizeMB / avgTime) * 1000).toFixed(1)),
      memoryDelta: this.memory.peak - this.memory.start.heapUsed,
      memoryPeak: this.memory.peak,
      times: [...this.times]
    };
  }
}

// Individual parser implementations
const parsers = {
  'csv-batch': {
    async parse(csvContent) {
      const results = await csvBatch(createStreamFromString(csvContent));
      return results.data;
    }
  },

  'csv-batch (sync map)': {
    async parse(csvContent) {
      const results = await csvBatch(createStreamFromString(csvContent), {
        map: record => record // Synchronous map to avoid async overhead
      });
      return results.data;
    }
  },

  'csv-parser': {
    parse(csvContent) {
      return new Promise((resolve, reject) => {
        const results = [];
        createStreamFromString(csvContent)
          .pipe(csv())
          .on('data', row => results.push(row))
          .on('end', () => resolve(results))
          .on('error', reject);
      });
    }
  },

  'fast-csv': {
    parse(csvContent) {
      return new Promise((resolve, reject) => {
        const results = [];
        fastcsv
          .parseString(csvContent, { 
            headers: true,
            objectMode: true,
            ignoreEmpty: true,
            trim: true
          })
          .on('data', row => results.push(row))
          .on('end', () => resolve(results))
          .on('error', reject);
      });
    }
  },

  'papaparse': {
    parse(csvContent) {
      return new Promise((resolve, reject) => {
        Papa.parse(csvContent, {
          header: true,
          complete: (results) => {
            if (results.errors.length > 0) {
              reject(new Error(results.errors[0].message));
            } else {
              resolve(results.data);
            }
          },
          error: reject
        });
      });
    }
  },

  'csv-parse': {
    parse(csvContent) {
      return new Promise((resolve, reject) => {
        const results = [];
        const parser = parse({ columns: true });
        
        parser.on('readable', function() {
          let record;
          while ((record = parser.read()) !== null) {
            results.push(record);
          }
        });
        
        parser.on('end', () => resolve(results));
        parser.on('error', reject);
        
        parser.write(csvContent);
        parser.end();
      });
    }
  },

  // Top performers from uDSV benchmark
  'uDSV (stringObjs)': {
    parse(csvContent) {
      try {
        const { inferSchema, initParser } = require('udsv');
        const schema = inferSchema(csvContent);
        const parser = initParser(schema);
        // Use stringObjs for optimal performance - returns objects directly
        return parser.stringObjs(csvContent);
      } catch (error) {
        throw new Error(`uDSV not installed: ${error.message}`);
      }
    }
  },

  'uDSV (stringArrs)': {
    parse(csvContent) {
      try {
        const { inferSchema, initParser } = require('udsv');
        const schema = inferSchema(csvContent);
        const parser = initParser(schema);
        const rows = parser.stringArrs(csvContent);
        
        // Convert arrays to objects (for comparison)
        if (rows.length === 0) return [];
        const headers = rows[0];
        const results = [];
        for (let i = 1; i < rows.length; i++) {
          const obj = {};
          headers.forEach((header, index) => {
            obj[header] = rows[i][index] || '';
          });
          results.push(obj);
        }
        return results;
      } catch (error) {
        throw new Error(`uDSV not installed: ${error.message}`);
      }
    }
  },

  'csv-simple-parser': {
    parse(csvContent) {
      try {
        const parseCsv = require('csv-simple-parser').default;
        // Use header: true for optimal performance - returns objects directly
        const result = parseCsv(csvContent, { header: true });
        return result;
      } catch (error) {
        throw new Error(`csv-simple-parser not installed: ${error.message}`);
      }
    }
  },

  'but-csv': {
    async parse(csvContent) {
      try {
        // Handle ESM-only module with dynamic import
        const { parse } = await import('but-csv');
        const rows = parse(csvContent);
        
        // Convert arrays to objects
        if (rows.length === 0) return [];
        const headers = rows[0];
        const results = [];
        for (let i = 1; i < rows.length; i++) {
          const obj = {};
          headers.forEach((header, index) => {
            obj[header] = rows[i][index] || '';
          });
          results.push(obj);
        }
        return results;
      } catch (error) {
        throw new Error(`but-csv not installed: ${error.message}`);
      }
    }
  },


  'd3-dsv': {
    parse(csvContent) {
      try {
        const d3 = require('d3-dsv');
        const results = d3.csvParse(csvContent);
        return results;
      } catch (error) {
        throw new Error(`d3-dsv not installed: ${error.message}`);
      }
    }
  },


  'comma-separated-values': {
    parse(csvContent) {
      try {
        const CSV = require('comma-separated-values');
        const rows = CSV.parse(csvContent);
        
        // Convert arrays to objects
        if (rows.length === 0) return [];
        const headers = rows[0];
        const results = [];
        for (let i = 1; i < rows.length; i++) {
          const obj = {};
          headers.forEach((header, index) => {
            obj[header] = rows[i][index] || '';
          });
          results.push(obj);
        }
        return results;
      } catch (error) {
        throw new Error(`comma-separated-values not installed: ${error.message}`);
      }
    }
  },

  // Raw string parsing for baseline comparison
  'String.split() (arrays)': {
    parse(csvContent) {
      // Pure array output like uDSV benchmark - maximum speed
      const lines = csvContent.split('\n');
      const results = [];
      
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].length > 0) {
          results.push(lines[i].split(','));
        }
      }
      
      return results;
    }
  },

  'String.split() (objects)': {
    parse(csvContent) {
      const lines = csvContent.split('\n');
      const rows = [];
      
      // Filter and split in one pass
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].length > 0) {
          rows.push(lines[i].split(','));
        }
      }
      
      if (rows.length === 0) return [];
      
      const headers = rows[0];
      const results = [];
      
      // Pre-allocate and use faster loops
      for (let i = 1; i < rows.length; i++) {
        const obj = {};
        const values = rows[i];
        for (let j = 0; j < headers.length; j++) {
          obj[headers[j]] = values[j] || '';
        }
        results.push(obj);
      }
      
      return results;
    }
  }
};

async function benchmarkFile(csvPath, rounds = 3) {
  console.log(`\nBenchmarking: ${path.basename(csvPath)}`);
  console.log('='.repeat(50));

  if (!fs.existsSync(csvPath)) {
    console.error(`File not found: ${csvPath}`);
    return null;
  }

  const csvContent = fs.readFileSync(csvPath, 'utf8');
  const fileSizeMB = (Buffer.byteLength(csvContent, 'utf8') / 1024 / 1024).toFixed(2);
  
  // Count rows (approximate)
  const rowCount = csvContent.split('\n').length - 1; // -1 for header
  
  console.log(`File size: ${fileSizeMB} MB, ~${rowCount.toLocaleString()} rows`);

  const results = [];
  
  for (const [parserName, parser] of Object.entries(parsers)) {
    console.log(`Testing ${parserName}...`);
    
    const measure = new PerfMeasure(parserName);
    let sampleOutput = null;
    let error = null;

    try {
      // Run multiple rounds for more accurate timing
      for (let round = 0; round < rounds; round++) {
        measure.start();
        const output = await parser.parse(csvContent);
        measure.end();
        
        if (round === 0) {
          sampleOutput = output.slice(0, 2); // Keep first 2 records as sample
        }
      }

      const stats = measure.getStats(csvContent, rowCount);
      stats.sample = sampleOutput;
      stats.error = null;
      results.push(stats);

    } catch (err) {
      console.log(`  Error: ${err.message}`);
      results.push({
        name: parserName,
        error: err.message,
        avgTime: null,
        rowsPerSec: null,
        mbPerSec: null,
        memoryDelta: null,
        sample: null
      });
    }
  }

  return {
    file: path.basename(csvPath),
    fileSizeMB: parseFloat(fileSizeMB),
    rowCount,
    results: results.sort((a, b) => {
      if (a.error && !b.error) return 1;
      if (!a.error && b.error) return -1;
      if (a.error && b.error) return 0;
      return a.avgTime - b.avgTime; // Sort by speed (fastest first)
    })
  };
}

async function runInMemoryBenchmarks() {
  console.log('CSV Parser In-Memory Performance Benchmarks');
  console.log('=' .repeat(60));
  console.log('Testing pure parsing performance (string → objects)');
  console.log('Based on uDSV\'s synthetic litmus test methodology\n');

  // Generate synthetic data if it doesn't exist
  const syntheticGen = require('../data/synthetic');
  syntheticGen.generateSyntheticDatasets();

  const testFiles = [
    'litmus_strings.csv',
    'litmus_ints.csv',
    'litmus_quoted.csv',
    'litmus_mixed.csv'
  ];

  const allResults = [];

  for (const file of testFiles) {
    const csvPath = path.join(__dirname, '..', 'data', file);
    const result = await benchmarkFile(csvPath, 3);
    if (result) {
      allResults.push(result);
    }
  }

  return allResults;
}

function formatResults(allResults) {
  const Table = require('cli-table3');

  for (const fileResult of allResults) {
    console.log(`\n${fileResult.file} (${fileResult.fileSizeMB} MB, ${fileResult.rowCount.toLocaleString()} rows)`);
    console.log('='.repeat(80));

    const table = new Table({
      head: ['Parser', 'Time (ms)', 'Rows/sec', 'MB/sec', 'Memory Δ', 'Status'],
      colWidths: [20, 12, 12, 10, 12, 10]
    });

    for (const result of fileResult.results) {
      if (result.error) {
        table.push([
          result.name,
          '-',
          '-', 
          '-',
          '-',
          'ERROR'
        ]);
      } else {
        const memoryMB = (result.memoryDelta / 1024 / 1024).toFixed(1);
        table.push([
          result.name,
          result.avgTime.toFixed(1),
          result.rowsPerSec.toLocaleString(),
          result.mbPerSec.toString(),
          `${memoryMB} MB`,
          '✓'
        ]);
      }
    }

    console.log(table.toString());

    // Show sample output from fastest parser
    const fastest = fileResult.results.find(r => !r.error);
    if (fastest && fastest.sample) {
      console.log(`\nSample output (${fastest.name}):`);
      console.log(JSON.stringify(fastest.sample[0], null, 2).slice(0, 200) + '...');
    }
  }
}

if (require.main === module) {
  runInMemoryBenchmarks()
    .then(formatResults)
    .catch(console.error);
}

module.exports = { runInMemoryBenchmarks, benchmarkFile };