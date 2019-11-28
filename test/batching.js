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

  it('with transform promise', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            resolve(batch);
          });
        });
      },
      transform: record => ({
        A: record.a,
        B: record.b,
        C: record.c
      })
    }).then(results => {
      // console.log(`results: ${JSON.stringify(results, null, 2)}`);
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        [
          {
            A: '1',
            B: '2',
            C: '3'
          }
        ],
        [
          {
            A: '4',
            B: '5',
            C: '6'
          }
        ]
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('with transform batch execution returning null', () => {
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
      },
      transform: record => ({
        A: record.a,
        B: record.b,
        C: record.c
      })
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });

  it('with transform batch execution returning empty', () => {
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
      },
      transform: record => Promise.resolve()
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });

  it('with transform batch execution rejecting', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            reject(new Error(`Error on call ${callCount}`));
          });
        });
      },
      transform: record => Promise.resolve()
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.isEmpty(results.data);
      assert.equal(results.errors.length, 2);
      assert.equal(results.errors[0].message, 'Error on call 1');
      assert.equal(results.errors[1].message, 'Error on call 2');
    });
  });

  it('with transform batch execution rejecting null', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    let callCount = 0;
    return csvBatch(createStreamFromString(csv), {
      batch: true,
      batchSize: 1,
      batchExecution: batch => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            callCount++;
            // eslint-disable-next-line prefer-promise-reject-errors
            reject(null);
          });
        });
      },
      transform: record => Promise.resolve()
    }).then(results => {
      assert.equal(callCount, 2);
      assert.equal(results.totalRecords, 2);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });
});
