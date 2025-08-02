'use strict';

const {assert} = require('chai');
const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('security features', () => {
  describe('field size limits', () => {
    it('respects maxFieldSize limit', async () => {
      const largeField = 'x'.repeat(1000);
      const csv = `a,b\n${largeField},small`;
      
      try {
        await csvBatch(createStreamFromString(csv), {maxFieldSize: 500});
        assert.fail('Expected security error');
      } catch (error) {
        assert.equal(error.name, 'CsvSecurityError');
        assert.equal(error.code, 'FIELD_SIZE_LIMIT_EXCEEDED');
        assert.include(error.message, 'Field size limit exceeded');
        assert.equal(error.line, 2);
      }
    });

    it('allows fields within size limit', async () => {
      const csv = 'a,b\nsmall,field';
      const results = await csvBatch(createStreamFromString(csv), {maxFieldSize: 100});
      assert.equal(results.totalRecords, 1);
      assert.equal(results.data[0].a, 'small');
    });

    it('handles null fields with size limits', async () => {
      const csv = 'a,b\n,small';
      const results = await csvBatch(createStreamFromString(csv), {
        maxFieldSize: 100,
        nullOnEmpty: true
      });
      assert.equal(results.totalRecords, 1);
      assert.isNull(results.data[0].a);
    });
  });

  describe('record size limits', () => {
    it('respects maxRecordSize limit', async () => {
      const field1 = 'x'.repeat(300);
      const field2 = 'y'.repeat(300);
      const field3 = 'z'.repeat(300);
      const csv = `a,b,c\n${field1},${field2},${field3}`;
      
      try {
        await csvBatch(createStreamFromString(csv), {maxRecordSize: 500});
        assert.fail('Expected security error');
      } catch (error) {
        assert.equal(error.name, 'CsvSecurityError');
        assert.equal(error.code, 'RECORD_SIZE_LIMIT_EXCEEDED');
        assert.include(error.message, 'Record size limit exceeded');
        assert.equal(error.line, 2);
      }
    });

    it('allows records within size limit', async () => {
      const csv = 'a,b,c\nsmall,medium,field';
      const results = await csvBatch(createStreamFromString(csv), {maxRecordSize: 1000});
      assert.equal(results.totalRecords, 1);
      assert.equal(results.data[0].a, 'small');
    });
  });

  describe('CSV injection prevention', () => {
    const dangerousValues = [
      '=SUM(1+1)',
      '+cmd|/c calc!A0',
      '-2+3+cmd|/c calc!A0',
      '@SUM(1+1)',
      '\t=SUM(1+1)'
      // Note: \r is excluded as it causes line breaks in CSV
    ];

    dangerousValues.forEach(dangerous => {
      it(`sanitizes dangerous value: ${dangerous.replace(/\t/g, '\\t').replace(/\r/g, '\\r')}`, async () => {
        const csv = `a,b\n${dangerous},safe`;
        const results = await csvBatch(createStreamFromString(csv), {
          preventCsvInjection: true
        });
        
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0].a, "'" + dangerous);
        assert.equal(results.data[0].b, 'safe');
      });
    });

    it('does not sanitize safe values', async () => {
      const safeValues = ['normal text', '123', 'text with spaces', 'under_score'];
      
      for (const safe of safeValues) {
        const csv = `a\n${safe}`;
        const results = await csvBatch(createStreamFromString(csv), {
          preventCsvInjection: true
        });
        
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0].a, safe);
      }
    });

    it('sanitizes carriage return prefix in quoted field', async () => {
      const csv = 'a\n"\r=SUM(1+1)"';
      const results = await csvBatch(createStreamFromString(csv), {
        preventCsvInjection: true
      });
      
      assert.equal(results.totalRecords, 1);
      assert.equal(results.data[0].a, "'\r=SUM(1+1)");
    });

    it('allows customizing injection prefixes', async () => {
      const csv = 'a,b\n=SUM(1+1),+cmd';
      const results = await csvBatch(createStreamFromString(csv), {
        preventCsvInjection: true,
        csvInjectionPrefixes: ['='] // Only = is dangerous in this config
      });
      
      assert.equal(results.totalRecords, 1);
      assert.equal(results.data[0].a, "'=SUM(1+1)"); // Sanitized
      assert.equal(results.data[0].b, '+cmd'); // Not sanitized
    });

    it('is disabled by default for backwards compatibility', async () => {
      const csv = 'a\n=SUM(1+1)';
      const results = await csvBatch(createStreamFromString(csv));
      
      assert.equal(results.totalRecords, 1);
      assert.equal(results.data[0].a, '=SUM(1+1)'); // Not sanitized
    });

    it('handles empty and null values correctly', async () => {
      const csv = 'a,b,c\n,=SUM(1+1),';
      const results = await csvBatch(createStreamFromString(csv), {
        preventCsvInjection: true,
        nullOnEmpty: true
      });
      
      assert.equal(results.totalRecords, 1);
      assert.isNull(results.data[0].a); // Empty -> null, not sanitized
      assert.equal(results.data[0].b, "'=SUM(1+1)"); // Dangerous -> sanitized
      assert.isNull(results.data[0].c); // Empty -> null, not sanitized
    });
  });

  describe('security options validation', () => {
    it('validates preventCsvInjection option', async () => {
      try {
        await csvBatch(createStreamFromString('a\n1'), {preventCsvInjection: 'yes'});
        assert.fail('Expected error');
      } catch (error) {
        assert.instanceOf(error, TypeError);
        assert.include(error.message, 'options.preventCsvInjection must be a boolean');
      }
    });

    it('validates csvInjectionPrefixes option', async () => {
      try {
        await csvBatch(createStreamFromString('a\n1'), {csvInjectionPrefixes: 'invalid'});
        assert.fail('Expected error');
      } catch (error) {
        assert.instanceOf(error, TypeError);
        assert.include(error.message, 'options.csvInjectionPrefixes must be an array');
      }
    });

    it('validates csvInjectionPrefixes array elements', async () => {
      try {
        await csvBatch(createStreamFromString('a\n1'), {csvInjectionPrefixes: ['=', 123, '+']});
        assert.fail('Expected error');
      } catch (error) {
        assert.instanceOf(error, TypeError);
        assert.include(error.message, 'options.csvInjectionPrefixes must be an array of strings');
      }
    });
  });

  describe('security with batching', () => {
    it('security errors stop processing immediately', async () => {
      const csv = 'a\n' + 'x'.repeat(1000) + '\nvalid';
      
      try {
        await csvBatch(createStreamFromString(csv), {
          maxFieldSize: 500,
          batch: true,
          batchSize: 1
        });
        assert.fail('Expected security error');
      } catch (error) {
        assert.equal(error.name, 'CsvSecurityError');
      }
    });

    it('CSV injection prevention works with batching', async () => {
      const csv = 'a\n=SUM(1+1)\nsafe\n+cmd';
      let batchCount = 0;
      
      const results = await csvBatch(createStreamFromString(csv), {
        preventCsvInjection: true,
        batch: true,
        batchSize: 2,
        batchExecution: batch => {
          batchCount++;
          return batch;
        }
      });
      
      assert.equal(results.totalRecords, 3);
      assert.equal(batchCount, 2); // 2 records in first batch, 1 in second
      assert.equal(results.data[0][0].a, "'=SUM(1+1)");
      assert.equal(results.data[0][1].a, 'safe');
      assert.equal(results.data[1][0].a, "'+cmd");
    });

    it('handles security errors in final processing', async () => {
      const largeField = 'x'.repeat(1000);
      const csv = `field\n${largeField}`; // No final newline to trigger final processing
      
      try {
        await csvBatch(createStreamFromString(csv), {
          maxFieldSize: 500
        });
        assert.fail('Should have thrown security error');
      } catch (error) {
        assert.equal(error.name, 'CsvSecurityError');
        assert.equal(error.code, 'FIELD_SIZE_LIMIT_EXCEEDED');
      }
    });

    it('handles CSV injection with custom prefixes', async () => {
      const csv = 'formula\n@SUM(1+1)\n%cmd';
      const results = await csvBatch(createStreamFromString(csv), {
        preventCsvInjection: true,
        csvInjectionPrefixes: ['@', '%', '=']
      });
      
      assert.equal(results.totalRecords, 2);
      assert.equal(results.data[0].formula, "'@SUM(1+1)"); // Sanitized
      assert.equal(results.data[1].formula, "'%cmd"); // Sanitized
    });

    it('handles empty values with CSV injection prevention', async () => {
      const csv = 'value\n\n@SUM(1+1)';
      const results = await csvBatch(createStreamFromString(csv), {
        preventCsvInjection: true,
        nullOnEmpty: true
      });
      
      assert.equal(results.totalRecords, 2);
      assert.equal(results.data[0].value, null); // Empty, not dangerous
      assert.equal(results.data[1].value, "'@SUM(1+1)"); // Sanitized
    });
  });
});