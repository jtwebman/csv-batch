import * as fs from 'fs';
import csvBatch = require('../index');
import { CsvBatchOptions, CsvBatchResults, CsvError, DetailedRecord, CsvSecurityError } from '../index';

// Test basic function import and usage
async function testBasicUsage() {
  const stream = fs.createReadStream('test/fixtures/basic.csv');
  
  // Basic usage with default types
  const results1: CsvBatchResults = await csvBatch(stream);
  const totalRecords: number = results1.totalRecords;
  const data: any = results1.data;
  const errors: CsvError[] = results1.errors;
}

// Test all option types
async function testAllOptions() {
  const stream = fs.createReadStream('test/fixtures/basic.csv');
  
  const options: CsvBatchOptions = {
    header: true,
    columns: ['name', 'age', 'city'],
    delimiter: ',',
    quote: '"',
    detail: false,
    nullOnEmpty: true,
    map: (record: any) => record,
    batch: false,
    batchSize: 1000,
    batchExecution: async (batch: any[]) => batch,
    getInitialValue: () => [],
    reducer: (acc: any[], record: any) => {
      acc.push(record);
      return acc;
    },
    maxFieldSize: 1024 * 1024,
    maxRecordSize: 10 * 1024 * 1024,
    preventCsvInjection: true,
    csvInjectionPrefixes: ['=', '+', '-', '@']
  };
  
  const results = await csvBatch(stream, options);
  
  // Test that we can access all result properties
  const totalRecords: number = results.totalRecords;
  const data: any = results.data;
  const errors: CsvError[] = results.errors;
}

// Test error types
function testErrorTypes() {
  // Test that CsvSecurityError can be instantiated with correct types
  const error = new CsvSecurityError(
    'Field too large',
    'FIELD_SIZE_LIMIT_EXCEEDED',
    5,
    10
  );
  
  const name: 'CsvSecurityError' = error.name;
  const code: 'FIELD_SIZE_LIMIT_EXCEEDED' | 'RECORD_SIZE_LIMIT_EXCEEDED' = error.code;
  const line: number = error.line;
  const column: number = error.column;
  const message: string = error.message;
}

// Test CsvError interface  
function testCsvErrorInterface() {
  const error: CsvError = {
    line: 1,
    column: 0,
    category: 'mapping',
    error: new Error('Test error'),
    userMessage: 'User friendly message',
    context: {
      recordNumber: 0,
      currentRecord: ['value1', 'value2'],
      fieldCount: 2,
      rawContent: 'value1,value2'
    },
    timestamp: new Date().toISOString()
  };
  
  // Test accessing properties
  const line: number = error.line;
  const category: string = error.category;
  const context = error.context;
  const recordNumber: number = context.recordNumber;
}

// Test DetailedRecord interface
function testDetailedRecordInterface() {
  const detailedRecord: DetailedRecord<{name: string}> = {
    line: 1,
    raw: 'John',
    data: { name: 'John' }
  };
  
  const line: number = detailedRecord.line;
  const raw: string = detailedRecord.raw;
  const data: {name: string} = detailedRecord.data;
}

// Test backwards compatibility with different column types
async function testColumnsCompatibility() {
  const stream = fs.createReadStream('test/fixtures/basic.csv');
  
  // Test with string array
  await csvBatch(stream, {
    columns: ['name', 'age', 'city']
  });
  
  // Test with number array (backwards compatibility)
  await csvBatch(stream, {
    columns: [0, 1, 2]
  });
  
  // Test with false (backwards compatibility)
  await csvBatch(stream, {
    columns: false
  });
}

// Test batch mode
async function testBatchMode() {
  const stream = fs.createReadStream('test/fixtures/basic.csv');
  
  const results = await csvBatch(stream, {
    batch: true,
    batchSize: 10,
    batchExecution: (batch: any[]) => `processed ${batch.length} records`
  });
  
  const data: any = results.data;  // Should be array of batch results
}

// Export functions to avoid unused code warnings
export {
  testBasicUsage,
  testAllOptions,
  testErrorTypes,
  testCsvErrorInterface,
  testDetailedRecordInterface,
  testColumnsCompatibility,
  testBatchMode
};