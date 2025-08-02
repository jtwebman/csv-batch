'use strict';

const fs = require('fs');
const csv = require('csv-parser');
const Profiler = require('../../../lib/profiler');

const BATCH_SIZE = 10000;

/**
 * csv-parser implementation for SQLite
 * 
 * Uses csv-parser's streaming interface with manual batching.
 * Pauses the stream during database writes to prevent overwhelming the database.
 * 
 * Optimization opportunities for csv-parser authors:
 * - The pause/resume mechanism could potentially be optimized
 * - Batch size tuning might improve performance
 */
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
          
          if (batch.length >= BATCH_SIZE) {
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

module.exports = benchmarkCsvParser;