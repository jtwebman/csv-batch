'use strict';

const fs = require('fs');
const csvBatch = require('../../../../index');
const Profiler = require('../../../lib/profiler');

const BATCH_SIZE = 10000;

/**
 * csv-batch implementation for SQLite
 * 
 * Uses csv-batch's built-in batching functionality with SQLite's insertMany method.
 * This leverages csv-batch's streaming parser with configurable batch execution.
 */
async function benchmarkCsvBatch(csvPath, db, totalRecords) {
  return Profiler.measure('csv-batch', totalRecords, async (profiler) => {
    const results = await csvBatch(fs.createReadStream(csvPath), {
      batch: true,
      batchSize: BATCH_SIZE,
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

module.exports = benchmarkCsvBatch;