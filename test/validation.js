'use strict';

const {assert} = require('chai');
const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

// Helper function to test validation errors
async function expectValidationError(fn, errorType, messageContains) {
  try {
    await fn();
    assert.fail('Expected error to be thrown');
  } catch (error) {
    assert.instanceOf(error, errorType);
    assert.include(error.message, messageContains);
  }
}

describe('input validation', () => {
  describe('stream validation', () => {
    it('throws error for null stream', async () => {
      await expectValidationError(
        () => csvBatch(null),
        TypeError,
        'First argument must be a readable stream'
      );
    });

    it('throws error for undefined stream', async () => {
      await expectValidationError(
        () => csvBatch(undefined),
        TypeError,
        'First argument must be a readable stream'
      );
    });

    it('throws error for non-stream object', async () => {
      await expectValidationError(
        () => csvBatch({}),
        TypeError,
        'First argument must be a readable stream'
      );
    });

    it('throws error for object without on method', async () => {
      await expectValidationError(
        () => csvBatch({pipe: () => {}}),
        TypeError,
        'readStream must have an "on" method'
      );
    });
  });

  describe('options validation', () => {
    it('throws error for non-object options', async () => {
      await expectValidationError(
        () => csvBatch(createStreamFromString('a,b,c\n1,2,3'), 'invalid'),
        TypeError,
        'Options must be an object'
      );
    });

    it('throws error for null options', async () => {
      await expectValidationError(
        () => csvBatch(createStreamFromString('a,b,c\n1,2,3'), null),
        TypeError,
        'Options must be an object'
      );
    });

    it('accepts undefined options (uses defaults)', async () => {
      const stream = createStreamFromString('a,b,c\n1,2,3');
      const results = await csvBatch(stream);
      assert.equal(results.totalRecords, 1);
    });

    it('accepts empty options object', async () => {
      const stream = createStreamFromString('a,b,c\n1,2,3');
      const results = await csvBatch(stream, {});
      assert.equal(results.totalRecords, 1);
    });

    describe('type validation', () => {
      it('validates header option', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('a\n1'), {header: 'true'}),
          TypeError,
          'options.header must be a boolean'
        );
      });

      it('validates columns as array or false', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {columns: 'a,b,c'}),
          TypeError,
          'options.columns must be an array or false'
        );
      });

      it('validates columns array elements are strings or numbers', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {columns: ['a', {}, 'c']}),
          TypeError,
          'options.columns must be an array of strings or numbers'
        );
      });

      it('accepts columns as false (backwards compatibility)', async () => {
        const stream = createStreamFromString('1,2,3');
        const results = await csvBatch(stream, {header: false, columns: false});
        assert.equal(results.totalRecords, 1);
        assert.deepEqual(results.data[0], ['1', '2', '3']);
      });

      it('accepts columns with numbers (backwards compatibility)', async () => {
        const stream = createStreamFromString('1,2,3');
        const results = await csvBatch(stream, {header: false, columns: [1, 2, 3]});
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0][1], '1');
      });

      it('validates delimiter is string', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {delimiter: 123}),
          TypeError,
          'options.delimiter must be a string'
        );
      });

      it('validates delimiter is single character', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {delimiter: ',,'}),
          RangeError,
          'options.delimiter must be exactly one character'
        );
      });

      it('validates quote is string', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {quote: 123}),
          TypeError,
          'options.quote must be a string'
        );
      });

      it('validates quote is single character', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {quote: '""'}),
          RangeError,
          'options.quote must be exactly one character'
        );
      });

      it('validates batchSize is integer', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {batchSize: 3.14}),
          TypeError,
          'options.batchSize must be an integer'
        );
      });

      it('validates batchSize is positive', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {batchSize: -1}),
          RangeError,
          'options.batchSize must be a positive integer'
        );
      });

      it('validates batchSize maximum', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {batchSize: 2000000}),
          RangeError,
          'options.batchSize must not exceed 1,000,000'
        );
      });

      it('validates function options', async () => {
        const tests = [
          ['map', 'not a function'],
          ['batchExecution', 'not a function'],
          ['getInitialValue', 'not a function'],
          ['reducer', 'not a function']
        ];

        for (const [option, value] of tests) {
          await expectValidationError(
            () => csvBatch(createStreamFromString('1'), {[option]: value}),
            TypeError,
            `options.${option} must be a function`
          );
        }
      });
    });

    describe('logical constraints', () => {
      it('prevents delimiter and quote being the same', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {delimiter: '"', quote: '"'}),
          Error,
          'options.delimiter and options.quote cannot be the same character'
        );
      });
    });

    describe('security options', () => {
      it('validates maxFieldSize', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {maxFieldSize: 3.14}),
          TypeError,
          'options.maxFieldSize must be an integer'
        );

        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {maxFieldSize: -1}),
          RangeError,
          'options.maxFieldSize must be a positive integer'
        );
      });

      it('validates maxRecordSize', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {maxRecordSize: 3.14}),
          TypeError,
          'options.maxRecordSize must be an integer'
        );

        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {maxRecordSize: -1}),
          RangeError,
          'options.maxRecordSize must be a positive integer'
        );

        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {maxRecordSize: 'invalid'}),
          TypeError,
          'options.maxRecordSize must be an integer'
        );
      });

      it('validates batchSize edge cases', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {batchSize: 'invalid'}),
          TypeError,
          'options.batchSize must be an integer'
        );
      });

      it('validates detail option type', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {detail: 'invalid'}),
          TypeError,
          'options.detail must be a boolean'
        );
      });

      it('validates nullOnEmpty option type', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {nullOnEmpty: 'invalid'}),
          TypeError,
          'options.nullOnEmpty must be a boolean'
        );
      });

      it('validates batch option type', async () => {
        await expectValidationError(
          () => csvBatch(createStreamFromString('1'), {batch: 'invalid'}),
          TypeError,
          'options.batch must be a boolean'
        );
      });
    });

    describe('successful validation', () => {
      it('accepts valid columns array', async () => {
        const stream = createStreamFromString('1,2,3');
        const results = await csvBatch(stream, {header: false, columns: ['a', 'b', 'c']});
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0].a, '1');
      });

      it('accepts valid security options', async () => {
        const stream = createStreamFromString('a,b\n1,2');
        const results = await csvBatch(stream, {
          maxFieldSize: 1000,
          maxRecordSize: 5000
        });
        assert.equal(results.totalRecords, 1);
      });
    });
  });
});