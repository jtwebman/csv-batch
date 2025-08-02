'use strict';

const fs = require('fs');
const csvBatch = require('../../../../index');
const Profiler = require('../../../lib/profiler');

const BATCH_SIZE = 5000; // Smaller batches for PostgreSQL network overhead

/**
 * csv-batch implementation for PostgreSQL
 * 
 * Uses csv-batch's built-in batching with PostgreSQL's insertBatch method.
 * Batch size is reduced for PostgreSQL due to network and transaction overhead.
 * 
 * Performance characteristics:
 * - Excellent streaming performance
 * - Built-in error handling and progress tracking
 * - Configurable batch execution
 * - Memory efficient for large files
 */
async function benchmarkCsvBatch(csvPath, db, totalRecords) {
  return Profiler.measure('csv-batch', totalRecords, async (profiler) => {
    const results = await csvBatch(fs.createReadStream(csvPath), {
      batch: true,
      batchSize: BATCH_SIZE,
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

module.exports = benchmarkCsvBatch;