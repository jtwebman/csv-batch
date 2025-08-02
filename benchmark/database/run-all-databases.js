'use strict';

const { runBenchmarks: runSQLite } = require('./sqlite-import');
const { runBenchmarks: runSQLiteOptimized } = require('./sqlite-optimized-import');
const { runBenchmarks: runPostgres } = require('./postgres-import');
const { runBenchmarks: runPostgresCopy } = require('./postgres-copy-import');
const { runBenchmarks: runMySQL } = require('./mysql-import');
const { runBenchmarks: runMySQLLoadData } = require('./mysql-load-data-import');

async function runAllDatabaseBenchmarks(size = 'medium') {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`DATABASE IMPORT BENCHMARKS - ${size.toUpperCase()} DATASET`);
  console.log(`${'='.repeat(80)}\n`);

  // SQLite benchmarks
  console.log('🗄️  SQLITE BENCHMARKS (INSERTS)');
  console.log('='.repeat(50));
  try {
    await runSQLite(size);
  } catch (error) {
    console.error('SQLite benchmark failed:', error.message);
  }

  console.log('\n');

  // SQLite optimized benchmarks
  console.log('🗄️  SQLITE BENCHMARKS (OPTIMIZED)');
  console.log('='.repeat(50));
  try {
    await runSQLiteOptimized(size);
  } catch (error) {
    console.error('SQLite optimized benchmark failed:', error.message);
  }

  console.log('\n');

  // PostgreSQL benchmarks  
  console.log('🐘 POSTGRESQL BENCHMARKS (INSERTS)');
  console.log('='.repeat(50));
  try {
    await runPostgres(size);
  } catch (error) {
    console.error('PostgreSQL benchmark failed:', error.message);
  }

  console.log('\n');

  // PostgreSQL COPY benchmarks
  console.log('🐘 POSTGRESQL BENCHMARKS (COPY)');
  console.log('='.repeat(50));
  try {
    await runPostgresCopy(size);
  } catch (error) {
    console.error('PostgreSQL COPY benchmark failed:', error.message);
  }

  console.log('\n');

  // MySQL benchmarks
  console.log('🐬 MYSQL BENCHMARKS (INSERTS)');
  console.log('='.repeat(50));
  try {
    await runMySQL(size);
  } catch (error) {
    console.error('MySQL benchmark failed:', error.message);
  }

  console.log('\n');

  // MySQL LOAD DATA benchmarks
  console.log('🐬 MYSQL BENCHMARKS (LOAD DATA INFILE)');
  console.log('='.repeat(50));
  try {
    await runMySQLLoadData(size);
  } catch (error) {
    console.error('MySQL LOAD DATA benchmark failed:', error.message);
  }

  console.log('\n');
  console.log(`${'='.repeat(80)}`);
  console.log('DATABASE BENCHMARKS COMPLETE');
  console.log(`${'='.repeat(80)}`);
}

if (require.main === module) {
  const size = process.argv[2] || 'medium';
  runAllDatabaseBenchmarks(size).catch(console.error);
}

module.exports = { runAllDatabaseBenchmarks };