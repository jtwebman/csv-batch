'use strict';

const {assert} = require('chai');

const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('reducer', () => {
  it('lets you reduce on parsed records', () => {
    const csv = `year,month,amount\n2020,12,34\n2020,12,-15\n2021,1,22`;
    return csvBatch(createStreamFromString(csv), {
      getInitialValue: () => ({}),
      reducer: (current, record) => {
        const amount = parseInt(record.amount, 10);
        if (!current[record.year]) {
          current[record.year] = {};
        }
        if (!current[record.year][record.month]) {
          current[record.year][record.month] = 0;
        }
        current[record.year][record.month] = current[record.year][record.month] + amount;
        return current;
      }
    }).then(results => {
      assert.equal(results.totalRecords, 3);
      assert.deepEqual(results.data, {
        '2020': {
          '12': 19
        },
        '2021': {
          '1': 22
        }
      });
      assert.isEmpty(results.errors);
    });
  });

  it('handles error thrown but still works for other rows', () => {
    const csv = `year,month,amount\n2020,12,34\n2020,12,bad\n2021,1,22`;
    return csvBatch(createStreamFromString(csv), {
      getInitialValue: () => ({}),
      reducer: (current, record) => {
        const amount = parseInt(record.amount, 10);
        if (isNaN(amount)) {
          throw new Error(`Amount "${record.amount}" was not a integer`);
        }
        if (!current[record.year]) {
          current[record.year] = {};
        }
        if (!current[record.year][record.month]) {
          current[record.year][record.month] = 0;
        }
        current[record.year][record.month] = current[record.year][record.month] + amount;
        return current;
      }
    }).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, {
        '2020': {
          '12': 34
        },
        '2021': {
          '1': 22
        }
      });
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].line, 3);
      assert.equal(results.errors[0].error.message, 'Amount "bad" was not a integer');
    });
  });

  it('handles reducer returning a promise', () => {
    const csv = `year,month,amount\n2020,12,34\n2021,1,22\n2020,12,-15`;
    return csvBatch(createStreamFromString(csv), {
      getInitialValue: () => ({}),
      reducer: (current, record, index) => {
        return new Promise(resolve => {
          setTimeout(() => {
            const amount = parseInt(record.amount, 10);
            if (!current[record.year]) {
              current[record.year] = {};
            }
            if (!current[record.year][record.month]) {
              current[record.year][record.month] = 0;
            }
            current[record.year][record.month] = current[record.year][record.month] + amount;
            resolve(current);
          });
        });
      }
    }).then(results => {
      assert.equal(results.totalRecords, 3);
      assert.deepEqual(results.data, {
        '2020': {
          '12': 19
        },
        '2021': {
          '1': 22
        }
      });
      assert.isEmpty(results.errors);
    });
  });

  it('handles reducer returning a promise that rejects', () => {
    const csv = `year,month,amount\n2020,12,34\n2021,1,22\n2020,12,bad`;
    return csvBatch(createStreamFromString(csv), {
      getInitialValue: () => ({}),
      reducer: (current, record, index) => {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            const amount = parseInt(record.amount, 10);
            if (isNaN(amount)) {
              return reject(new Error(`Amount "${record.amount}" was not a integer`));
            }
            if (!current[record.year]) {
              current[record.year] = {};
            }
            if (!current[record.year][record.month]) {
              current[record.year][record.month] = 0;
            }
            current[record.year][record.month] = current[record.year][record.month] + amount;
            resolve(current);
          });
        });
      }
    }).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, {
        '2020': {
          '12': 34
        },
        '2021': {
          '1': 22
        }
      });
      assert.equal(results.errors.length, 1);
      assert.equal(results.errors[0].line, 4);
      assert.equal(results.errors[0].error.message, 'Amount "bad" was not a integer');
    });
  });
});
