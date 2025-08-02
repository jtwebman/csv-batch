'use strict';

const fs = require('fs');
const { parse } = require('csv-parse');
const Profiler = require('../../../lib/profiler');

const BATCH_SIZE = 10000;

/**
 * csv-parse implementation for SQLite
 * 
 * Uses csv-parse's readable stream interface with manual batching.
 * Reads records individually and batches them for database insertion.
 * 
 * Optimization opportunities for csv-parse authors:
 * - The readable pattern works well for controlled processing
 * - Could potentially benefit from built-in batching functionality
 * - Stream backpressure handling is good
 */
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

module.exports = benchmarkCsvParse;