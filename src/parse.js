'use strict';

const {Writable} = require('stream');
const {StringDecoder} = require('string_decoder');

/**
 * If we have columns convert array to object with properties set as the columns
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @return {Array|Object} - returns either a object or the context currentRecord passed in
 */
function makeRecord(options, context) {
  if (Array.isArray(options.columns)) {
    const columnsLength = options.columns.length;
    if (columnsLength > 0) {
      const recordLength = context.currentRecord.length;
      const newRecord = {};
      for (let i = 0; i < recordLength; i++) {
        if (i < columnsLength) {
          newRecord[options.columns[i]] = context.currentRecord[i];
        } else {
          break;
        }
      }
      if (options.detail) {
        return {
          line: context.currentLine,
          raw: context.currentRaw.join(''),
          data: newRecord
        };
      } else {
        return newRecord;
      }
    }
  }
  if (options.detail) {
    return {
      line: context.currentLine,
      raw: context.currentRaw.join(''),
      data: context.currentRecord
    };
  } else {
    return context.currentRecord;
  }
}

/**
 * Compares two things and if they are both strings ignores casing and accents
 * @param {*} a
 * @param {*} b
 * @return {Boolean} - returns true if the are equal
 */
function ciEquals(a, b) {
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b, undefined, {sensitivity: 'accent'}) === 0;
  } else {
    return a === b;
  }
}

/**
 * Test if this is the header row based on column
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @return {Boolean} - returns true if it is the header
 */
function isHeader(options, context) {
  if (context.currentLine != 1) {
    // not first line never the header
    return false;
  }
  if (context.currentLine === 1 && options.header) {
    // first line is header use as columns
    options.columns = context.currentRecord;
    return true;
  }
  if (!Array.isArray(options.columns)) {
    return false;
  }
  const columnsLength = options.columns.length;
  if (columnsLength <= 0) {
    return false;
  }
  const recordLength = context.currentRecord.length;
  if (columnsLength != recordLength) {
    return false;
  }
  for (let i = 0; i < recordLength; i++) {
    if (!ciEquals(options.columns[i], context.currentRecord[i])) {
      return false;
    }
  }
  return true;
}

/**
 * Add a value to the record
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 */
function addToRecord(options, context) {
  if (options.nullOnEmpty && !context.currentValueHadEmpty && context.currentValue.join('').trim() === '') {
    context.currentRecord.push(null);
  } else {
    context.currentRecord.push(context.currentValue.join(''));
  }
  context.currentValueHadEmpty = false;
  context.currentValue = [];
}

/**
 * gets a transform promise if options.transform is set
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 * @return {Promise} - a promise that wrappes the transform function
 */
function getTransformPromise(options, context, batchResults) {
  return new Promise((resolve, reject) => {
    try {
      resolve(options.transform(makeRecord(options, context)));
    } catch (error) {
      reject(error);
    }
  }).catch(error => {
    if (typeof error !== 'undefined' && error !== null) {
      batchResults.errors.push(error);
    }
  });
}

/**
 * Finish the record adding the batch in not the header
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 */
function finishRecord(options, context, batchResults) {
  if (!isHeader(options, context)) {
    if (options.transform) {
      context.batch.push(getTransformPromise(options, context, batchResults));
    } else {
      context.batch.push(makeRecord(options, context));
    }
  }
  context.currentRecord = [];
  context.currentRaw = [];
}

/**
 * Resolves all the records and then filters out any null or undefined values
 * @param {Array.<Promise>} records - records that need to be resolved and filtered
 * @return {Promise} - resolves with the values filtered
 */
function resolveBatchTransforms(records) {
  return Promise.all(records).then(values => values.filter(value => typeof value !== 'undefined' && value !== null));
}

/**
 * gets a batch execution promise handling the success and failure
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 * @return {Promise} - a promise that resolves adding the the batchResults
 */
function getBatchPromise(options, context, batchResults) {
  if (options.transform) {
    return resolveBatchTransforms(context.batch)
      .then(options.batchExecution)
      .then(result => {
        if (typeof result !== 'undefined' && result !== null) {
          batchResults.data.push(result);
        }
      })
      .catch(error => {
        if (typeof error !== 'undefined' && error !== null) {
          batchResults.errors.push(error);
        }
      });
  }
  return new Promise((resolve, reject) => {
    try {
      resolve(options.batchExecution(context.batch));
    } catch (error) {
      reject(error);
    }
  })
    .then(result => {
      if (typeof result !== 'undefined' && result !== null) {
        batchResults.data.push(result);
      }
    })
    .catch(error => {
      if (typeof error !== 'undefined' && error !== null) {
        batchResults.errors.push(error);
      }
    });
}

/**
 * Finish the record adding the batch in not the header
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 * @param {Array.<Promise>} batchTasks - batch tasks
 */
function doBatching(options, context, batchResults, batchTasks) {
  if (options.batch && context.batch.length >= options.batchSize) {
    batchResults.totalRecords = batchResults.totalRecords + context.batch.length;
    batchTasks.push(getBatchPromise(options, context, batchResults));
    context.batch = [];
  }
}

/**
 * Writable stream to parse csv into batches that execute
 * @param {Object} options - csv parsing options
 * @return {Object} - a new row csv writable stream
 */
function parse(options) {
  const context = {
    currentLine: 1,
    currentValue: [],
    currentRecord: [],
    currentRaw: [],
    currentValueHadEmpty: false,
    inQuote: false,
    batch: []
  };
  const batchResults = {
    totalRecords: 0,
    data: [],
    errors: []
  };
  return new Writable({
    writableObjectMode: true,
    write(chunk, encoding, callback) {
      const batchTasks = [];
      let value = chunk;
      if (encoding === 'buffer') {
        const decoder = new StringDecoder();
        value = decoder.write(chunk);
      }
      const valueLength = value.length;
      for (let i = 0; i < valueLength; i++) {
        const cc = value.charAt(i);
        const nc = value.charAt(i + 1);

        if (cc === options.quote && context.inQuote && nc === options.quote) {
          context.currentValue.push(options.quote);
          context.currentRaw.push(cc, nc);
          i++;
          continue;
        }

        if (cc === options.quote && !context.inQuote && nc === options.quote) {
          context.currentValueHadEmpty = true;
        }

        if (cc === options.quote) {
          context.inQuote = !context.inQuote;
          context.currentRaw.push(cc);
          continue;
        }

        if (cc === options.delimiter && !context.inQuote) {
          addToRecord(options, context);
          context.currentRaw.push(cc);
          continue;
        }

        if (cc === '\r' && nc === '\n' && !context.inQuote) {
          addToRecord(options, context);
          finishRecord(options, context, batchResults);
          doBatching(options, context, batchResults, batchTasks);
          i++;
          context.currentLine++;
          continue;
        }

        if ((cc === '\n' || cc === '\r') && !context.inQuote) {
          addToRecord(options, context);
          finishRecord(options, context, batchResults);
          doBatching(options, context, batchResults, batchTasks);
          context.currentLine++;
          continue;
        }

        if ((cc === '\r' && nc === '\n') || cc === '\n' || cc === '\r') {
          context.currentLine++;
        }

        context.currentValue.push(cc);
        context.currentRaw.push(cc);
      }
      if (batchTasks.length > 0) {
        Promise.all(batchTasks).finally(() => {
          callback();
        });
      } else {
        callback();
      }
    },
    final(callback) {
      if (context.currentRecord.length > 0 || context.currentValue.length > 0) {
        addToRecord(options, context);
        finishRecord(options, context, batchResults);
      }
      if (options.batch && context.batch.length > 0) {
        batchResults.totalRecords = batchResults.totalRecords + context.batch.length;
        getBatchPromise(options, context, batchResults).finally(() => {
          this.emit('results', batchResults);
          callback();
        });
      } else {
        if (!options.batch) {
          batchResults.totalRecords = context.batch.length;
          batchResults.data = context.batch;
        }
        if (options.transform) {
          resolveBatchTransforms(context.batch).then(batch => {
            batchResults.data = batch;
            this.emit('results', batchResults);
            callback();
          });
        } else {
          this.emit('results', batchResults);
          callback();
        }
      }
    }
  });
}

module.exports = parse;
