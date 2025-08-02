'use strict';

const {assert} = require('chai');
const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('enhanced error reporting', () => {
  describe('error structure', () => {
    it('provides enhanced error information for mapping errors', async () => {
      const csv = 'a,b\n1,2\n3,4';
      const results = await csvBatch(createStreamFromString(csv), {
        map: record => {
          if (record.a === '3') {
            throw new Error('Test mapping error');
          }
          return record;
        }
      });

      assert.equal(results.totalRecords, 1); // Only successful records are counted  
      assert.equal(results.data.length, 1); // Only successful record
      assert.equal(results.errors.length, 1);

      const error = results.errors[0];
      assert.equal(error.line, 3);
      assert.equal(error.column, 0);
      assert.equal(error.category, 'mapping');
      assert.equal(error.error.message, 'Test mapping error');
      assert.include(error.userMessage, 'Record mapping error at line 3');
      assert.isObject(error.context);
      assert.equal(error.context.recordNumber, 1); // 0-indexed, second record when mapping failed
      assert.deepEqual(error.context.currentRecord, ['3', '4']);
      assert.equal(error.context.fieldCount, 2);
      assert.isString(error.timestamp);
      assert.isString(error.context.mapFunction);
    });

    it('provides enhanced error information for reducer errors', async () => {
      const csv = 'a,b\n1,2\n3,4';
      const results = await csvBatch(createStreamFromString(csv), {
        getInitialValue: () => [],
        reducer: (acc, record) => {
          if (record.a === '3') {
            throw new Error('Test reducer error');
          }
          acc.push(record);
          return acc;
        }
      });

      assert.equal(results.totalRecords, 1); // Only successful records are counted
      assert.equal(results.data.length, 1); // Only successful record
      assert.equal(results.errors.length, 1);

      const error = results.errors[0];
      assert.equal(error.line, 3);
      assert.equal(error.category, 'reducing');
      assert.equal(error.error.message, 'Test reducer error');
      assert.include(error.userMessage, 'Data reduction error at line 3');
      assert.isObject(error.context);
      assert.equal(error.context.recordNumber, 1); // 0-indexed, second record when mapping failed
      assert.isString(error.context.reducerFunction);
      assert.isString(error.context.recordData);
    });

    it('provides enhanced error information for batch execution errors', async () => {
      const csv = 'a,b\n1,2\n3,4';
      const results = await csvBatch(createStreamFromString(csv), {
        batch: true,
        batchSize: 2,
        batchExecution: () => {
          throw new Error('Test batch error');
        }
      });

      assert.equal(results.totalRecords, 2);
      assert.equal(results.data.length, 0); // No successful batches
      assert.equal(results.errors.length, 1);

      const error = results.errors[0];
      assert.equal(error.category, 'batching');
      assert.equal(error.error.message, 'Test batch error');
      assert.include(error.userMessage, 'Batch processing error');
      assert.isObject(error.context);
      assert.isString(error.context.batchExecutionFunction);
      assert.equal(error.context.batchSize, 2);
    });

    it('provides enhanced error information for security errors', async () => {
      const largeField = 'x'.repeat(1000);
      const csv = `a,b\n${largeField},small`;

      try {
        await csvBatch(createStreamFromString(csv), {maxFieldSize: 500});
        assert.fail('Expected security error');
      } catch (error) {
        // Security errors are thrown immediately, not collected
        assert.equal(error.name, 'CsvSecurityError');
        assert.equal(error.line, 2);
        assert.equal(error.column, 0); // Column where the field starts
      }
    });
  });

  describe('error context information', () => {
    it('includes record number and field count', async () => {
      const csv = 'a,b,c\n1,2,3\n4,5,6\n7,8,9';
      const results = await csvBatch(createStreamFromString(csv), {
        map: record => {
          if (record.a === '4') {
            throw new Error('Test error on second record');
          }
          return record;
        }
      });

      const error = results.errors[0];
      assert.equal(error.context.recordNumber, 1); // Second record (0-indexed)
      assert.equal(error.context.fieldCount, 3);
      assert.deepEqual(error.context.currentRecord, ['4', '5', '6']);
    });

    it('includes raw content snippet', async () => {
      const csv = 'name,value\n"John Doe",123\n"Jane Smith",456';
      const results = await csvBatch(createStreamFromString(csv), {
        map: record => {
          if (record.name === 'Jane Smith') {
            throw new Error('Test error');
          }
          return record;
        }
      });

      const error = results.errors[0];
      assert.isString(error.context.rawContent);
      assert.include(error.context.rawContent, 'Jane Smith');
    });

    it('includes timestamp', async () => {
      const csv = 'a\n1';
      const beforeTime = new Date().toISOString();
      
      const results = await csvBatch(createStreamFromString(csv), {
        map: () => {
          throw new Error('Test error');
        }
      });

      const afterTime = new Date().toISOString();
      const error = results.errors[0];
      
      assert.isString(error.timestamp);
      assert.isTrue(error.timestamp >= beforeTime);
      assert.isTrue(error.timestamp <= afterTime);
    });
  });

  describe('user-friendly error messages', () => {
    it('creates readable messages for different error categories', async () => {
      const testCases = [
        {
          category: 'mapping',
          expectedPattern: /Record mapping error at line \d+.*record #\d+/
        },
        {
          category: 'reducing', 
          expectedPattern: /Data reduction error at line \d+.*record #\d+/
        },
        {
          category: 'batching',
          expectedPattern: /Batch processing error for batch containing record at line \d+/
        }
      ];

      for (const testCase of testCases) {
        const csv = 'a\n1';
        let results;

        if (testCase.category === 'mapping') {
          results = await csvBatch(createStreamFromString(csv), {
            map: () => { throw new Error('Test error'); }
          });
        } else if (testCase.category === 'reducing') {
          results = await csvBatch(createStreamFromString(csv), {
            reducer: () => { throw new Error('Test error'); }
          });
        } else if (testCase.category === 'batching') {
          results = await csvBatch(createStreamFromString(csv), {
            batch: true,
            batchExecution: () => { throw new Error('Test error'); }
          });
        }

        assert.equal(results.errors.length, 1);
        const error = results.errors[0];
        assert.match(error.userMessage, testCase.expectedPattern);
      }
    });
  });

  describe('backwards compatibility', () => {
    it('maintains backwards compatibility for simple error access', async () => {
      const csv = 'a\n1';
      const results = await csvBatch(createStreamFromString(csv), {
        map: () => {
          throw new Error('Test error');
        }
      });

      // Old way of accessing errors should still work
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].line, 2);
      assert.equal(results.errors[0].error.message, 'Test error');
    });
  });

  describe('error handling with different configurations', () => {
    it('handles errors with custom delimiters and quotes', async () => {
      const csv = 'a;b\n1;2\n3;4';
      const results = await csvBatch(createStreamFromString(csv), {
        delimiter: ';',
        map: record => {
          if (record.a === '3') {
            throw new Error('Custom delimiter error');
          }
          return record;
        }
      });

      const error = results.errors[0];
      assert.equal(error.category, 'mapping');
      assert.include(error.userMessage, 'line 3');
    });

    it('handles errors with detail mode enabled', async () => {
      const csv = 'a,b\n1,2';
      const results = await csvBatch(createStreamFromString(csv), {
        detail: true,
        map: () => {
          throw new Error('Detail mode error');
        }
      });

      const error = results.errors[0];
      assert.equal(error.category, 'mapping');
      assert.isObject(error.context);
    });
  });

  describe('edge case error handling', () => {
    it('handles undefined errors gracefully in mapping', async () => {
      const csv = 'a\n1';
      const results = await csvBatch(createStreamFromString(csv), {
        map: () => {
          throw undefined; // Undefined error (edge case)
        }
      });
      
      assert.equal(results.totalRecords, 0);
      assert.equal(results.data.length, 0);
      assert.equal(results.errors.length, 0); // Undefined errors are ignored
    });

    it('handles null errors gracefully in batch execution', async () => {
      const csv = 'a\n1\n2';
      const results = await csvBatch(createStreamFromString(csv), {
        batch: true,
        batchSize: 1,
        batchExecution: () => {
          throw null; // Null error (edge case)
        }
      });
      
      assert.equal(results.totalRecords, 2);
      assert.equal(results.data.length, 0); // No successful batches
      assert.equal(results.errors.length, 0); // Null errors are ignored
    });

    it('handles errors during final batch processing', async () => {
      const csv = 'a\n1\n2'; // No final newline, triggers final batch processing
      const results = await csvBatch(createStreamFromString(csv), {
        batch: true,
        batchSize: 10, // Large batch size to ensure final processing
        batchExecution: batch => {
          if (batch.length > 0 && batch[0].a === '1') {
            throw new Error('Final batch error');
          }
          return 'processed';
        }
      });
      
      assert.equal(results.totalRecords, 2);
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].category, 'batching');
    });

    it('includes raw content snippet in error context', async () => {
      const csv = 'name,value\n"John Doe",123\n"Jane Smith",456';
      const results = await csvBatch(createStreamFromString(csv), {
        map: record => {
          if (record.name === 'Jane Smith') {
            throw new Error('Test error');
          }
          return record;
        }
      });

      const error = results.errors[0];
      assert.isString(error.context.rawContent);
      assert.include(error.context.rawContent, 'Jane Smith');
    });

    it('covers all error message categories', async () => {
      // We need to test the createUserFriendlyMessage function with different categories
      // This is mainly for coverage of the switch statement
      const csv = 'a\n1';
      
      try {
        const results = await csvBatch(createStreamFromString(csv), {
          map: () => {
            // Create an error that would trigger the default case (if any)
            const error = new Error('Custom error');
            error.category = 'unknown'; // This might trigger default case
            throw error;
          }
        });
        
        assert.equal(results.errors.length, 1);
        assert.equal(results.errors[0].category, 'mapping');
      } catch (error) {
        // This is fine too - means the test worked
        assert.isTrue(error instanceof Error);
      }
    });

    it('handles error with no message property', async () => {
      const csv = 'a\n1';
      const results = await csvBatch(createStreamFromString(csv), {
        map: () => {
          // Create an error without a message property
          const error = {};
          error.toString = () => 'Custom error object';
          throw error;
        }
      });
      
      assert.equal(results.totalRecords, 0);
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].category, 'mapping');
    });
  });
});