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
  if (options.header) {
    // first line is header use as columns
    if (options.columns.length === 0) {
      options.columns = context.currentRecord;
    }
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
 * Gets the record and then calls the map function
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 * @return {Promise<*>} - a promise that resolves to the map function
 */
async function getMapRecord(options, context, batchResults) {
  try {
    return await options.map(makeRecord(options, context));
  } catch (error) {
    if (typeof error !== 'undefined' && error !== null) {
      batchResults.errors.push({
        line: context.currentLine,
        error
      });
    }
  }
}

/**
 * Finish the record adding the batch in ignoring the header
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 */
async function finishRecord(options, context, batchResults) {
  if (!isHeader(options, context)) {
    const record = await getMapRecord(options, context, batchResults);
    if (typeof record !== 'undefined' && record !== null) {
      try {
        const index = context.processedRecords + 1;
        context.batch = await options.reducer(context.batch, record, index);
        context.processedRecords = index;
        context.batchRecords = context.batchRecords + 1;
      } catch (error) {
        batchResults.errors.push({
          line: context.currentLine,
          error
        });
      }
    }
  }
  context.currentRecord = [];
  context.currentRaw = [];
}

/**
 * gets a batch execution promise handling the success and failure
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 * @return {Promise} - a promise that resolves adding the the batchResults
 */
async function getBatch(options, context, batchResults) {
  try {
    const result = await options.batchExecution(context.batch);
    if (typeof result !== 'undefined' && result !== null) {
      batchResults.data.push(result);
    }
  } catch (error) {
    if (typeof error !== 'undefined' && error !== null) {
      batchResults.errors.push(error);
    }
  }
}

/**
 * Finish the record adding the batch in not the header
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @param {Object} batchResults - final results object
 */
async function doBatching(options, context, batchResults) {
  if (options.batch && context.batchRecords >= options.batchSize) {
    await getBatch(options, context, batchResults);
    context.batchRecords = 0;
    context.batch = options.getInitialValue();
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
    batch: options.getInitialValue(),
    batchRecords: 0,
    processedRecords: 0
  };
  const batchResults = {
    totalRecords: 0,
    data: [],
    errors: []
  };
  const decoder = new StringDecoder();
  return new Writable({
    write: async (chunk, encoding, callback) => {
      try {
        let value = chunk;
        if (encoding === 'buffer') {
          value = decoder.end(chunk);
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
            await finishRecord(options, context, batchResults);
            await doBatching(options, context, batchResults);
            i++;
            context.currentLine++;
            continue;
          }

          if ((cc === '\n' || cc === '\r') && !context.inQuote) {
            addToRecord(options, context);
            await finishRecord(options, context, batchResults);
            await doBatching(options, context, batchResults);
            context.currentLine++;
            continue;
          }

          if ((cc === '\r' && nc === '\n') || cc === '\n' || cc === '\r') {
            context.currentLine++;
          }

          context.currentValue.push(cc);
          context.currentRaw.push(cc);
        }
        callback();
      } catch (error) {
        callback(error);
      }
    },
    final: async function (callback) {
      try {
        if (context.currentRecord.length > 0 || context.currentValue.length > 0) {
          addToRecord(options, context);
          await finishRecord(options, context, batchResults);
        }
        if (options.batch && context.batchRecords > 0) {
          await getBatch(options, context, batchResults);
        }
        if (!options.batch) {
          batchResults.data = context.batch;
        }
        batchResults.totalRecords = context.processedRecords;
        this.emit('results', batchResults);
        callback();
      } catch (error) {
        callback(error);
      }
    }
  });
}

module.exports = parse;
