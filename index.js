'use strict';

const parse = require('./src/parse');

/**
 * Validates the options object and throws descriptive errors for invalid configurations
 * @param {Object} options - The options object to validate
 * @throws {TypeError|RangeError} When validation fails
 */
function validateOptions(options) {
  if (options == null || typeof options !== 'object') {
    throw new TypeError('Options must be an object');
  }

  // Validate header option
  if (options.header !== undefined && typeof options.header !== 'boolean') {
    throw new TypeError('options.header must be a boolean');
  }

  // Validate columns option (allow false for backward compatibility, but warn about non-strings)
  if (options.columns !== undefined) {
    if (options.columns !== false && !Array.isArray(options.columns)) {
      throw new TypeError('options.columns must be an array or false');
    }
    if (Array.isArray(options.columns) && options.columns.some(col => 
      typeof col !== 'string' && typeof col !== 'number')) {
      throw new TypeError('options.columns must be an array of strings or numbers');
    }
  }

  // Validate delimiter option
  if (options.delimiter !== undefined) {
    if (typeof options.delimiter !== 'string') {
      throw new TypeError('options.delimiter must be a string');
    }
    if (options.delimiter.length !== 1) {
      throw new RangeError('options.delimiter must be exactly one character');
    }
  }

  // Validate quote option
  if (options.quote !== undefined) {
    if (typeof options.quote !== 'string') {
      throw new TypeError('options.quote must be a string');
    }
    if (options.quote.length !== 1) {
      throw new RangeError('options.quote must be exactly one character');
    }
  }

  // Validate detail option
  if (options.detail !== undefined && typeof options.detail !== 'boolean') {
    throw new TypeError('options.detail must be a boolean');
  }

  // Validate nullOnEmpty option
  if (options.nullOnEmpty !== undefined && typeof options.nullOnEmpty !== 'boolean') {
    throw new TypeError('options.nullOnEmpty must be a boolean');
  }

  // Validate map option
  if (options.map !== undefined && typeof options.map !== 'function') {
    throw new TypeError('options.map must be a function');
  }

  // Validate batch option
  if (options.batch !== undefined && typeof options.batch !== 'boolean') {
    throw new TypeError('options.batch must be a boolean');
  }

  // Validate batchSize option
  if (options.batchSize !== undefined) {
    if (!Number.isInteger(options.batchSize)) {
      throw new TypeError('options.batchSize must be an integer');
    }
    if (options.batchSize <= 0) {
      throw new RangeError('options.batchSize must be a positive integer');
    }
    if (options.batchSize > 1000000) {
      throw new RangeError('options.batchSize must not exceed 1,000,000 for memory safety');
    }
  }

  // Validate batchExecution option
  if (options.batchExecution !== undefined && typeof options.batchExecution !== 'function') {
    throw new TypeError('options.batchExecution must be a function');
  }

  // Validate getInitialValue option
  if (options.getInitialValue !== undefined && typeof options.getInitialValue !== 'function') {
    throw new TypeError('options.getInitialValue must be a function');
  }

  // Validate reducer option
  if (options.reducer !== undefined && typeof options.reducer !== 'function') {
    throw new TypeError('options.reducer must be a function');
  }

  // Validate security options (if present)
  if (options.maxFieldSize !== undefined) {
    if (!Number.isInteger(options.maxFieldSize)) {
      throw new TypeError('options.maxFieldSize must be an integer');
    }
    if (options.maxFieldSize <= 0) {
      throw new RangeError('options.maxFieldSize must be a positive integer');
    }
  }

  if (options.maxRecordSize !== undefined) {
    if (!Number.isInteger(options.maxRecordSize)) {
      throw new TypeError('options.maxRecordSize must be an integer');
    }
    if (options.maxRecordSize <= 0) {
      throw new RangeError('options.maxRecordSize must be a positive integer');
    }
  }

  if (options.preventCsvInjection !== undefined && typeof options.preventCsvInjection !== 'boolean') {
    throw new TypeError('options.preventCsvInjection must be a boolean');
  }

  if (options.csvInjectionPrefixes !== undefined) {
    if (!Array.isArray(options.csvInjectionPrefixes)) {
      throw new TypeError('options.csvInjectionPrefixes must be an array');
    }
    if (options.csvInjectionPrefixes.some(prefix => typeof prefix !== 'string')) {
      throw new TypeError('options.csvInjectionPrefixes must be an array of strings');
    }
  }

  // Validate logical constraints (only check if both are defined)
  if (options.delimiter !== undefined && options.quote !== undefined && 
      options.delimiter === options.quote) {
    throw new Error('options.delimiter and options.quote cannot be the same character');
  }

  if (options.batch === true && options.batchExecution === undefined) {
    console.warn(
      'Warning: batch mode enabled but no batchExecution function provided. ' +
      'Batches will be collected in memory.'
    );
  }
}

const defaultOptions = {
  header: true,
  columns: [],
  delimiter: ',',
  quote: '"',
  detail: false,
  nullOnEmpty: false,
  map: record => record,
  batch: false,
  batchSize: 10000,
  batchExecution: batch => batch,
  getInitialValue: () => [],
  reducer: (current, record) => {
    current.push(record);
    return current;
  },
  // Security options
  maxFieldSize: 1024 * 1024, // 1MB per field
  maxRecordSize: 10 * 1024 * 1024, // 10MB per record
  preventCsvInjection: false, // Disabled by default for backwards compatibility
  csvInjectionPrefixes: ['=', '+', '-', '@', '\t', '\r'] // Common CSV injection prefixes
};

/**
 * CSV parse and then batch
 * @param {NodeJS.ReadableStream} readStream - a readable stream
 * @param {Object} [overrides={}] - overrides default options
 * @return {Promise<Object>} - a promise that resolves when all the batches finish or rejects if there was an error
 * @throws {TypeError} When readStream is not a readable stream or options are invalid
 * @throws {RangeError} When option values are out of valid range
 */
async function csvBatch(readStream, overrides = {}) {
  // Validate readStream
  if (!readStream || typeof readStream.pipe !== 'function') {
    throw new TypeError('First argument must be a readable stream with a pipe method');
  }

  if (typeof readStream.on !== 'function') {
    throw new TypeError('readStream must have an "on" method for event handling');
  }

  // Validate and merge options
  validateOptions(overrides);
  const options = Object.assign({}, defaultOptions, overrides);
  return new Promise((resolve, reject) => {
    const parser = parse(options);
    let results = null;
    parser.on('finish', () => {
      resolve(results);
    });
    parser.on('results', result => {
      results = result;
    });
    parser.on('error', reject);
    readStream.pipe(parser);
  });
}
module.exports = csvBatch;
