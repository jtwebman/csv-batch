'use strict';

const {Writable} = require('stream');
const {StringDecoder} = require('string_decoder');

/**
 * Security error class for CSV-related security violations
 */
class CsvSecurityError extends Error {
  constructor(message, code, line, column) {
    super(message);
    this.name = 'CsvSecurityError';
    this.code = code;
    this.line = line;
    this.column = column;
  }
}

/**
 * Enhanced error information with additional context
 */
function createEnhancedError(error, context, category, details = {}) {
  return {
    line: context.currentLine,
    column: context.currentColumn || 0,
    category: category, // 'parsing', 'mapping', 'reducing', 'batching', 'security'
    error: error,
    context: {
      recordNumber: (context.recordsAttempted || 1) - 1, // 0-indexed record number being processed
      currentRecord: context.currentRecord.length > 0 ? [...context.currentRecord] : null,
      fieldCount: context.currentRecord.length,
      rawContent: context.currentRaw.join('').slice(-50), // Last 50 chars of raw content
      ...details
    },
    timestamp: new Date().toISOString()
  };
}

/**
 * Creates a user-friendly error message based on error type
 */
function createUserFriendlyMessage(error, context, category) {
  const location = `line ${context.currentLine}, column ${context.currentColumn || 0}`;
  
  switch (category) {
  case 'parsing':
    return `CSV parsing error at ${location}: ${error.message || 'Invalid CSV format'}`;
  case 'mapping':
    return `Record mapping error at ${location} (record #${context.recordsAttempted || 1}): ${error.message}`;
  case 'reducing':
    return `Data reduction error at ${location} (record #${context.recordsAttempted || 1}): ${error.message}`;
  case 'batching':
    return `Batch processing error for batch containing record at ${location}: ${error.message}`;
  case 'security':
    return `Security violation at ${location}: ${error.message}`;
  default:
    return `Error at ${location}: ${error.message || 'Unknown error'}`;
  }
}

/**
 * Checks if a field value might be a CSV injection attempt
 * @param {string} value - the field value to check
 * @param {Array<string>} prefixes - array of dangerous prefixes
 * @return {boolean} - true if potentially dangerous
 */
function isPotentialCsvInjection(value, prefixes) {
  if (typeof value !== 'string' || value.length === 0) {
    return false;
  }
  
  const trimmedValue = value.trim();
  return prefixes.some(prefix => trimmedValue.startsWith(prefix));
}

/**
 * Sanitizes a potentially dangerous CSV value by prefixing with a single quote
 * @param {string} value - the value to sanitize
 * @return {string} - sanitized value
 */
function sanitizeCsvValue(value) {
  return "'" + value;
}

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
 * Add a value to the record with security checks
 * @param {Object} options - csv parsing options
 * @param {Object} context - context of the current parse state
 * @throws {CsvSecurityError} when security limits are exceeded
 */
function addToRecord(options, context) {
  let fieldValue;
  
  if (options.nullOnEmpty && !context.currentValueHadEmpty && context.currentValue.join('').trim() === '') {
    fieldValue = null;
  } else {
    fieldValue = context.currentValue.join('');
    
    // Security check: field size limit
    if (options.maxFieldSize && fieldValue && fieldValue.length > options.maxFieldSize) {
      throw new CsvSecurityError(
        `Field size limit exceeded: ${fieldValue.length} bytes > ${options.maxFieldSize} bytes`,
        'FIELD_SIZE_LIMIT_EXCEEDED',
        context.currentLine,
        0 // Column position at start of field
      );
    }
    
    // Security check: CSV injection prevention
    if (options.preventCsvInjection && isPotentialCsvInjection(fieldValue, options.csvInjectionPrefixes)) {
      fieldValue = sanitizeCsvValue(fieldValue);
    }
  }
  
  context.currentRecord.push(fieldValue);
  context.currentValueHadEmpty = false;
  context.currentValue = [];
  
  // Security check: record size limit (estimate)
  if (options.maxRecordSize) {
    const recordSize = context.currentRecord.reduce((size, field) => {
      return size + (field ? field.length : 0);
    }, 0);
    
    if (recordSize > options.maxRecordSize) {
      throw new CsvSecurityError(
        `Record size limit exceeded: ${recordSize} bytes > ${options.maxRecordSize} bytes`,
        'RECORD_SIZE_LIMIT_EXCEEDED',
        context.currentLine,
        0
      );
    }
  }
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
      // For mapping errors, the column should be 0 (start of record)
      const errorContext = {
        ...context,
        currentColumn: 0
      };
      const enhancedError = createEnhancedError(error, errorContext, 'mapping', {
        mapFunction: options.map.toString().slice(0, 100) + '...'
      });
      enhancedError.userMessage = createUserFriendlyMessage(error, errorContext, 'mapping');
      batchResults.errors.push(enhancedError);
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
    // Increment the number of records attempted (for error context)
    context.recordsAttempted = (context.recordsAttempted || 0) + 1;
    
    const record = await getMapRecord(options, context, batchResults);
    if (typeof record !== 'undefined' && record !== null) {
      try {
        const index = context.processedRecords + 1;
        context.batch = await options.reducer(context.batch, record, index);
        context.processedRecords = index;
        context.batchRecords = context.batchRecords + 1;
      } catch (error) {
        // For reducing errors, the column should be 0 (start of record)
        const errorContext = {
          ...context,
          currentColumn: 0
        };
        const enhancedError = createEnhancedError(error, errorContext, 'reducing', {
          reducerFunction: options.reducer.toString().slice(0, 100) + '...',
          currentBatchSize: context.batchRecords,
          recordData: JSON.stringify(record).slice(0, 200) + '...'
        });
        enhancedError.userMessage = createUserFriendlyMessage(error, errorContext, 'reducing');
        batchResults.errors.push(enhancedError);
      }
    }
  }
  context.currentRecord = [];
  context.currentRaw = [];
  context.currentColumn = 0;
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
      // For backwards compatibility, if the error is a simple string or has no enhanced context needed,
      // store it as-is. Otherwise use enhanced error format.
      if (typeof error === 'string' || (error && typeof error !== 'object')) {
        batchResults.errors.push(error);
      } else {
        const enhancedError = createEnhancedError(error, context, 'batching', {
          batchExecutionFunction: options.batchExecution.toString().slice(0, 100) + '...',
          batchSize: Array.isArray(context.batch) ? context.batch.length : 'unknown',
          currentBatchRecords: context.batchRecords
        });
        enhancedError.userMessage = createUserFriendlyMessage(error, context, 'batching');
        batchResults.errors.push(enhancedError);
      }
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
    currentColumn: 0,
    currentValue: [],
    currentRecord: [],
    currentRaw: [],
    currentValueHadEmpty: false,
    inQuote: false,
    batch: options.getInitialValue(),
    batchRecords: 0,
    processedRecords: 0,
    recordsAttempted: 0
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
        
        // Pre-compute character codes for faster comparison
        const quoteCode = options.quote.charCodeAt(0);
        const delimiterCode = options.delimiter.charCodeAt(0);
        const crCode = 13; // '\r'
        const lfCode = 10; // '\n'
        
        // Helper function to handle repeated error pattern
        const handleSecurityError = (securityError) => {
          if (securityError instanceof CsvSecurityError) {
            const enhancedError = createEnhancedError(securityError, context, 'security');
            enhancedError.userMessage = createUserFriendlyMessage(securityError, context, 'security');
            batchResults.errors.push(enhancedError);
            throw securityError;
          }
          throw securityError;
        };
        
        for (let i = 0; i < valueLength; i++) {
          const cc = value.charCodeAt(i);
          
          // Fast batched processing for runs of normal characters
          if (!context.inQuote && cc !== quoteCode && cc !== delimiterCode && cc !== crCode && cc !== lfCode) {
            // Look ahead to find the end of this run of normal characters
            let runEnd = i + 1;
            while (runEnd < valueLength) {
              const nextCode = value.charCodeAt(runEnd);
              if (nextCode === quoteCode || nextCode === delimiterCode || nextCode === crCode || nextCode === lfCode) {
                break;
              }
              runEnd++;
            }
            
            // Add the entire run at once
            const run = value.slice(i, runEnd);
            context.currentValue.push(run);
            context.currentRaw.push(run);
            context.currentColumn += run.length;
            
            // Skip to the end of the run
            i = runEnd - 1;
            continue;
          }
          
          const nc = i + 1 < valueLength ? value.charCodeAt(i + 1) : 0;

          if (cc === quoteCode && context.inQuote && nc === quoteCode) {
            context.currentValue.push(options.quote);
            context.currentRaw.push(String.fromCharCode(cc), String.fromCharCode(nc));
            i++;
            continue;
          }

          if (cc === quoteCode && !context.inQuote && nc === quoteCode) {
            context.currentValueHadEmpty = true;
          }

          if (cc === quoteCode) {
            context.inQuote = !context.inQuote;
            context.currentRaw.push(String.fromCharCode(cc));
            
            // If we just entered a quoted section, look ahead to the closing quote
            if (context.inQuote) {
              let quoteEnd = value.indexOf(options.quote, i + 1);
              if (quoteEnd !== -1) {
                // Check if it's an escaped quote (doubled)
                let nextQuoteIdx = quoteEnd + 1;
                while (nextQuoteIdx < valueLength && value.charCodeAt(nextQuoteIdx) === quoteCode) {
                  // Skip the escaped quote and look for the next one
                  quoteEnd = value.indexOf(options.quote, nextQuoteIdx + 1);
                  if (quoteEnd === -1) break;
                  nextQuoteIdx = quoteEnd + 1;
                }
                
                if (quoteEnd !== -1) {
                  // Extract the content between quotes efficiently
                  const quotedContent = value.slice(i + 1, quoteEnd);
                  context.currentValue.push(quotedContent);
                  context.currentRaw.push(options.quote + quotedContent + options.quote);
                  context.inQuote = false;
                  i = quoteEnd; // Will be incremented by loop
                  continue;
                }
              }
            }
            continue;
          }

          if (cc === delimiterCode && !context.inQuote) {
            try {
              addToRecord(options, context);
            } catch (securityError) {
              handleSecurityError(securityError);
            }
            context.currentRaw.push(String.fromCharCode(cc));
            context.currentColumn++;
            continue;
          }

          if (cc === crCode && nc === lfCode && !context.inQuote) {
            try {
              addToRecord(options, context);
            } catch (securityError) {
              handleSecurityError(securityError);
            }
            await finishRecord(options, context, batchResults);
            await doBatching(options, context, batchResults);
            i++;
            context.currentLine++;
            context.currentColumn = 0;
            continue;
          }

          if ((cc === lfCode || cc === crCode) && !context.inQuote) {
            try {
              addToRecord(options, context);
            } catch (securityError) {
              handleSecurityError(securityError);
            }
            await finishRecord(options, context, batchResults);
            await doBatching(options, context, batchResults);
            context.currentLine++;
            context.currentColumn = 0;
            continue;
          }

          if ((cc === crCode && nc === lfCode) || cc === lfCode || cc === crCode) {
            context.currentLine++;
            context.currentColumn = 0;
          } else {
            context.currentColumn++;
          }

          context.currentValue.push(String.fromCharCode(cc));
          context.currentRaw.push(String.fromCharCode(cc));
        }
        callback();
      } catch (error) {
        callback(error);
      }
    },
    final: async function (callback) {
      try {
        if (context.currentRecord.length > 0 || context.currentValue.length > 0) {
          try {
            addToRecord(options, context);
          } catch (securityError) {
            if (securityError instanceof CsvSecurityError) {
              const enhancedError = createEnhancedError(securityError, context, 'security');
              enhancedError.userMessage = createUserFriendlyMessage(securityError, context, 'security');
              batchResults.errors.push(enhancedError);
              throw securityError;
            }
            throw securityError;
          }
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
