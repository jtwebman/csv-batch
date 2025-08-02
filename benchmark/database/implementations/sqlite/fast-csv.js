'use strict';

const fastcsv = require('fast-csv');
const Profiler = require('../../../lib/profiler');

const BATCH_SIZE = 10000;

/**
 * fast-csv implementation for SQLite
 * 
 * Uses fast-csv's parseFile method with manual batching.
 * 
 * Optimization opportunities for fast-csv authors:
 * - The current implementation doesn't support stream pausing during batch processing
 * - Could benefit from built-in batching functionality
 * - Memory usage could be optimized for large files
 */
async function benchmarkFastCsv(csvPath, db, totalRecords) {
  return Profiler.measure('fast-csv', totalRecords, async (profiler) => {
    return new Promise((resolve, reject) => {
      let batch = [];
      let processed = 0;
      
      fastcsv
        .parseFile(csvPath, { headers: true })
        .on('data', (row) => {
          batch.push(row);
          
          if (batch.length >= BATCH_SIZE) {
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

module.exports = benchmarkFastCsv;