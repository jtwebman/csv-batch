'use strict';

const {assert} = require('chai');

const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('batching', () => {
  it('works with a none promise function', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        callCount++;
        return `processed batch ${callCount} size ${batch.length}`;
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, ['processed batch 1 size 1', 'processed batch 2 size 1']);
      assert.isEmpty(results.errors);
    });
  });

  it('works with a promise function', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            resolve(`processed batch ${callCount} size ${batch.length}`);
          });
        });
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, ['processed batch 1 size 1', 'processed batch 2 size 1']);
      assert.isEmpty(results.errors);
    });
  });

  it('works when batchExecution returns nothing', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            resolve();
          });
        });
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });

  it('works when batchExecution returns null', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            resolve(null);
          });
        });
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });

  it('handles batchExecution reject', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            if (callCount === 1) {
              // eslint-disable-next-line prefer-promise-reject-errors
              reject(`error on batch ${callCount} size ${batch.length}`);
            } else {
              resolve(`processed batch ${callCount} size ${batch.length}`);
            }
          });
        });
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, ['processed batch 2 size 1']);
      assert.deepEqual(results.errors, ['error on batch 1 size 1']);
    });
  });

  it('handles batchExecution throw error', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        callCount++;
        if (callCount === 1) {
          // eslint-disable-next-line no-throw-literal
          throw `error on batch ${callCount} size ${batch.length}`;
        }
        return `processed batch ${callCount} size ${batch.length}`;
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, ['processed batch 2 size 1']);
      assert.deepEqual(results.errors, ['error on batch 1 size 1']);
    });
  });

  it('handles batchExecution reject with null', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            if (callCount === 1) {
              // eslint-disable-next-line prefer-promise-reject-errors
              reject(null);
            } else {
              resolve(`processed batch ${callCount} size ${batch.length}`);
            }
          });
        });
      }
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, ['processed batch 2 size 1']);
      assert.isEmpty(results.errors);
    });
  });

  it('batchExecution not being set just returning the batches in data', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1
    }).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        [
          {
            a: '1',
            b: '2',
            c: '3'
          }
        ],
        [
          {
            a: '4',
            b: '5',
            c: '6'
          }
        ]
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('empty file', () => {
    return csvBatch(createStreamFromString(''), {
      batch: true,
      batchSize: 1
    }).then(results => {
      assert.equal(results.totalRecords, 0);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });
});
