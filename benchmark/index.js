#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// Import benchmark modules
const { runInMemoryBenchmarks } = require('./parsing/in-memory');
const sqliteBenchmark = require('./database/sqlite-import');
const postgresBenchmark = require('./database/postgres-import');
const mysqlBenchmark = require('./database/mysql-import');
const UDSVFormatter = require('./lib/udsv-formatter');

const BANNER = `
╔═══════════════════════════════════════════════════════════════╗
║                CSV Parser Benchmark Suite                     ║
║                                                               ║
║  uDSV-style synthetic tests + Real-world database imports    ║
║                                                               ║
║  Libraries: csv-batch, uDSV, d3-dsv, but-csv,               ║
║             csv-simple-parser, comma-separated-values + more ║
╚═══════════════════════════════════════════════════════════════╝
`;

function showHelp() {
  console.log(`
CSV Parser Benchmark Suite

Usage:
  node benchmark/index.js [type] [size]

Benchmark Types:
  parsing     - In-memory parsing benchmarks (uDSV-style)
  sqlite      - SQLite database import
  postgres    - PostgreSQL database import  
  mysql       - MySQL database import
  database    - All database imports
  all         - All benchmarks (default)

Dataset Sizes:
  small       - 100K records (~10 MB)
  medium      - 1M records (~100 MB, default)
  large       - 10M records (~1 GB)
  xlarge      - 100M records (~10 GB)

Examples:
  node benchmark/index.js                    # All benchmarks, medium size
  node benchmark/index.js parsing            # Only parsing tests
  node benchmark/index.js sqlite large       # SQLite with large dataset
  node benchmark/index.js database medium    # All DB tests, medium size

Notes:
  - PostgreSQL and MySQL require Docker (auto-started)
  - Test data is generated automatically if missing
  - Use --gc-interval flag for GC monitoring
`);
}

async function ensureDataExists(size, type = 'both') {
  console.log('Ensuring test data exists...');
  
  if (type === 'parsing' || type === 'both') {
    // Generate synthetic data for parsing tests
    const syntheticGen = require('./data/synthetic');
    const syntheticFiles = [
      path.join(__dirname, 'data', 'litmus_strings.csv'),
      path.join(__dirname, 'data', 'litmus_ints.csv'),
      path.join(__dirname, 'data', 'litmus_quoted.csv')
    ];
    
    const needsSynthetic = syntheticFiles.some(file => !fs.existsSync(file));
    if (needsSynthetic) {
      console.log('Generating synthetic datasets...');
      syntheticGen.generateSyntheticDatasets();
    }
  }
  
  if (type === 'database' || type === 'both') {
    // Generate realistic data for database tests
    const { fileSizes } = require('./data/generate-data');
    const config = fileSizes[size];
    const dataPath = path.join(__dirname, 'results', config.file);
    
    if (!fs.existsSync(dataPath)) {
      console.log(`Generating ${config.records.toLocaleString()} record dataset...`);
      execSync(`node ${path.join(__dirname, 'data', 'generate-data.js')} ${size}`, {
        stdio: 'inherit'
      });
    }
  }
}

async function checkDatabases() {
  try {
    execSync('docker-compose ps', { cwd: __dirname, stdio: 'pipe' });
    const output = execSync('docker-compose ps --services --filter "status=running"', { 
      cwd: __dirname 
    }).toString();
    
    const runningServices = output.trim().split('\n').filter(Boolean);
    const requiredServices = ['postgres', 'mysql'];
    const missingServices = requiredServices.filter(s => !runningServices.includes(s));
    
    if (missingServices.length > 0) {
      console.log('\n⚠️  Starting databases...');
      execSync('docker-compose up -d', { cwd: __dirname, stdio: 'inherit' });
      console.log('Waiting for databases to be ready...');
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
    return true;
  } catch (error) {
    console.log('\n⚠️  Docker not available. Database tests will be skipped.');
    return false;
  }
}

async function runParsingBenchmarks() {
  console.log('\n' + '═'.repeat(80));
  console.log('SYNTHETIC PARSING BENCHMARKS (uDSV-style)');
  console.log('═'.repeat(80));
  console.log('Testing pure CSV parsing performance (string → objects)');
  console.log('Based on uDSV\'s litmus test methodology\n');
  
  const results = await runInMemoryBenchmarks();
  
  // Format results in uDSV style
  for (const result of results) {
    const fileInfo = {
      file: result.file,
      fileSizeMB: result.fileSizeMB,
      rowCount: result.rowCount,
      cols: 20 // Our synthetic data has 20 columns
    };
    
    console.log('\n' + UDSVFormatter.formatBenchmarkTable(result.results, fileInfo));
  }
  
  return results;
}

async function runDatabaseBenchmarks(databases, size) {
  console.log('\n' + '═'.repeat(80));
  console.log('DATABASE INTEGRATION BENCHMARKS');
  console.log('═'.repeat(80));
  console.log(`Testing real-world CSV → Database import scenarios (${size} dataset)\n`);
  
  const results = {};
  
  for (const db of databases) {
    try {
      console.log(`\n${'─'.repeat(60)}`);
      console.log(`${db.toUpperCase()} IMPORT TEST`);
      console.log('─'.repeat(60));
      
      let benchmarkResult;
      switch (db) {
        case 'sqlite':
          benchmarkResult = await sqliteBenchmark.runBenchmarks(size);
          break;
        case 'postgres':
          benchmarkResult = await postgresBenchmark.runBenchmarks(size);
          break;
        case 'mysql':
          benchmarkResult = await mysqlBenchmark.runBenchmarks(size);
          break;
      }
      
      results[db] = benchmarkResult;
    } catch (error) {
      console.error(`Error in ${db} benchmark:`, error.message);
      results[db] = { error: error.message };
    }
  }
  
  return results;
}

async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    showHelp();
    return;
  }
  
  const benchmarkType = args[0] || 'all';
  const size = args[1] || 'medium';
  
  // Validate inputs
  const validTypes = ['parsing', 'sqlite', 'postgres', 'mysql', 'database', 'all'];
  const validSizes = ['small', 'medium', 'large', 'xlarge'];
  
  if (!validTypes.includes(benchmarkType)) {
    console.error(`Invalid benchmark type: ${benchmarkType}`);
    console.error(`Valid types: ${validTypes.join(', ')}`);
    return;
  }
  
  if (!validSizes.includes(size)) {
    console.error(`Invalid size: ${size}`);
    console.error(`Valid sizes: ${validSizes.join(', ')}`);
    return;
  }
  
  console.log(BANNER);
  console.log(`Benchmark Type: ${benchmarkType}`);
  console.log(`Dataset Size: ${size}`);
  console.log('='.repeat(60));
  
  const allResults = [];
  
  // Determine what to run
  const runParsing = ['parsing', 'all'].includes(benchmarkType);
  const runDatabase = ['database', 'all'].includes(benchmarkType) || 
                     ['sqlite', 'postgres', 'mysql'].includes(benchmarkType);
  
  // Ensure test data exists
  const dataType = runParsing && runDatabase ? 'both' : 
                  runParsing ? 'parsing' : 'database';
  await ensureDataExists(size, dataType);
  
  // Run parsing benchmarks
  if (runParsing) {
    const parsingResults = await runParsingBenchmarks();
    allResults.push(...parsingResults);
  }
  
  // Run database benchmarks
  if (runDatabase) {
    let databases;
    if (benchmarkType === 'sqlite') {
      databases = ['sqlite'];
    } else if (benchmarkType === 'postgres') {
      databases = ['postgres'];
    } else if (benchmarkType === 'mysql') {
      databases = ['mysql'];
    } else {
      databases = ['sqlite', 'postgres', 'mysql'];
    }
    
    // Check if we need Docker
    const needsDocker = databases.some(db => db !== 'sqlite');
    const dockerAvailable = needsDocker ? await checkDatabases() : true;
    
    // Filter databases based on Docker availability
    if (!dockerAvailable) {
      databases = databases.filter(db => db === 'sqlite');
      if (databases.length === 0) {
        console.log('No databases available to test.');
        return;
      }
    }
    
    const dbResults = await runDatabaseBenchmarks(databases, size);
    
    // Add database results to allResults for summary
    for (const [dbName, dbResult] of Object.entries(dbResults)) {
      if (dbResult && !dbResult.error && Array.isArray(dbResult)) {
        // dbResult is an array of benchmark results
        allResults.push({
          name: `${dbName.toUpperCase()} Database Import`,
          results: dbResult,
          isDatabase: true
        });
      }
    }
  }
  
  // Generate summary report
  if (allResults.length > 0) {
    console.log(UDSVFormatter.createSummaryReport(allResults));
    
    // Save markdown report
    const markdownReport = UDSVFormatter.generateMarkdownReport(allResults, benchmarkType, size);
    const timestamp = new Date().toISOString().slice(0, 19).replace(/[:-]/g, '');
    const filename = `benchmark-results-${benchmarkType}-${size}-${timestamp}.md`;
    const reportPath = path.join(__dirname, 'results', filename);
    
    // Ensure results directory exists
    const resultsDir = path.join(__dirname, 'results');
    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true });
    }
    
    fs.writeFileSync(reportPath, markdownReport);
    console.log(`\n📊 Benchmark report saved: ${reportPath}`);
  }
  
  console.log('\n✅ Benchmark complete!');
  console.log('\nTo run specific benchmarks:');
  console.log('  node benchmark/index.js parsing      # Pure parsing tests');
  console.log('  node benchmark/index.js database     # Database import tests');
  console.log('  node benchmark/index.js sqlite large # SQLite with large dataset');
}

if (require.main === module) {
  main().catch(error => {
    console.error('Benchmark failed:', error);
    process.exit(1);
  });
}

module.exports = { main };