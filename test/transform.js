'use strict';

const {assert} = require('chai');

const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('transform', () => {
  it('lets you alter each record', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {
      transform: record => ({
        a: Number.parseInt(record.a, 10),
        b: Number.parseInt(record.b, 10),
        c: Number.parseInt(record.c, 10)
      })
    }).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          a: 1,
          b: 2,
          c: 3
        },
        {
          a: 4,
          b: 5,
          c: 6
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('handles thrown error', () => {
    const csv = `a,b,c\n1,2,3`;
    return csvBatch(createStreamFromString(csv), {
      transform: () => {
        throw new Error('Error on transform');
      }
    }).then(results => {
      assert.equal(results.totalRecords, 1);
      assert.isEmpty(results.data);
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].message, 'Error on transform');
    });
  });

  it('handles rejected promise', () => {
    const csv = `a,b,c\n1,2,3`;
    return csvBatch(createStreamFromString(csv), {
      transform: () => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            reject(new Error('Error on transform'));
          });
        });
      }
    }).then(results => {
      assert.equal(results.totalRecords, 1);
      assert.isEmpty(results.data);
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].message, 'Error on transform');
    });
  });

  it('handles rejected promise with null', () => {
    const csv = `a,b,c\n1,2,3`;
    return csvBatch(createStreamFromString(csv), {
      transform: () => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            // eslint-disable-next-line prefer-promise-reject-errors
            reject(null);
          });
        });
      }
    }).then(results => {
      assert.equal(results.totalRecords, 1);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });
});
