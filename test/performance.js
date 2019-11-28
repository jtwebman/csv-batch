'use strict';

const {assert} = require('chai');
const {range} = require('lodash');

const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

const millionRowCsv = range(1000000)
  .map(i => `${i},${i + 1},${i + 2}`)
  .join('\n');

describe('performance', () => {
  it('test million in 10,000 batches', () => {
    let callCount = 0;
    return csvBatch(createStreamFromString(millionRowCsv), {
      header: false,
      batch: true,
      batchSize: 10000,
      batchExecution: batch => {
        callCount++;
        return `processed batch ${callCount} size ${batch.length}`;
      }
    }).then(results => {
      assert.equal(callCount, 100);
      assert.equal(results.totalRecords, 1000000);
      assert.equal(results.data.length, 100);
      assert.isEmpty(results.errors);
    });
  });

  it('test million no batching', () => {
    return csvBatch(createStreamFromString(millionRowCsv), {
      header: false
    }).then(results => {
      assert.equal(results.totalRecords, 1000000);
      assert.equal(results.data.length, 1000000);
      assert.isEmpty(results.errors);
    });
  });
});
