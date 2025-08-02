'use strict';

const fs = require('fs');
const Papa = require('papaparse');
const Profiler = require('../../../lib/profiler');

const BATCH_SIZE = 10000;

/**
 * papaparse implementation for SQLite
 * 
 * Uses papaparse's streaming mode with chunk-based processing.
 * The chunkSize is set larger than batch size to reduce overhead.
 * 
 * Optimization opportunities for papaparse authors:
 * - Chunk size tuning affects performance significantly
 * - The chunk callback approach works well for batching
 * - Memory usage is generally efficient in streaming mode
 */
async function benchmarkPapaParse(csvPath, db, totalRecords) {
  return Profiler.measure('papaparse', totalRecords, async (profiler) => {
    return new Promise((resolve, reject) => {
      let processed = 0;
      
      Papa.parse(fs.createReadStream(csvPath), {
        header: true,
        chunkSize: BATCH_SIZE * 100, // Approximate bytes per chunk
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

module.exports = benchmarkPapaParse;