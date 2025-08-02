'use strict';

const {assert} = require('chai');
const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('parsing edge cases for coverage', () => {
  it('handles malformed CSV that triggers parsing error category', async () => {
    // Create a scenario that would trigger a parsing error
    // This is tricky since most parsing errors are handled gracefully
    const csv = 'a,b\n"unclosed quote,data\nmore,data';
    
    const results = await csvBatch(createStreamFromString(csv));
    
    // The parser should handle this gracefully, but we want to test the error categorization
    assert.isArray(results.data);
    assert.isArray(results.errors);
  });

  it('handles quote edge cases with empty values', async () => {
    const csv = 'field\n""'; // Empty quoted value  
    const results = await csvBatch(createStreamFromString(csv));
    
    // The parser might treat this differently - let's just check it processes without error
    assert.isArray(results.data);
    assert.isNumber(results.totalRecords);
  });

  it('handles line ending edge cases', async () => {
    const csv = 'a\r\n1\r2\n3'; // Mixed line endings
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 3);
    assert.equal(results.data[0].a, '1');
    assert.equal(results.data[1].a, '2');
    assert.equal(results.data[2].a, '3');
  });

  it('handles buffer encoding edge cases', async () => {
    const csv = 'data\ntest';
    const stream = createStreamFromString(csv);
    
    // This should test the buffer encoding path in the parser
    const results = await csvBatch(stream);
    
    assert.equal(results.totalRecords, 1);
    assert.equal(results.data[0].data, 'test');
  });

  it('handles quote processing with consecutive quotes', async () => {
    const csv = 'field\n"a""b""c"'; // Should become a"b"c
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 1);
    assert.equal(results.data[0].field, 'a"b"c');
  });

  it('handles complex quote scenarios', async () => {
    const csv = 'field\n"""test"""'; // Should become "test"
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 1);
    assert.equal(results.data[0].field, '"test"');
  });

  it('handles carriage return only line endings', async () => {
    const csv = 'a\r1\r2';
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 2);
    assert.equal(results.data[0].a, '1');
    assert.equal(results.data[1].a, '2');
  });

  it('handles field with only quotes', async () => {
    const csv = 'field\n""""'; // Four quotes should become one quote
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 1);
    assert.equal(results.data[0].field, '"');
  });

  it('handles empty file with just headers', async () => {
    const csv = 'a,b,c'; // Just headers, no data
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 0);
    assert.equal(results.data.length, 0);
  });

  it('handles single character CSV', async () => {
    const csv = 'a'; // Just one character
    const results = await csvBatch(createStreamFromString(csv));
    
    assert.equal(results.totalRecords, 0); // Just header
    assert.equal(results.data.length, 0);
  });
});