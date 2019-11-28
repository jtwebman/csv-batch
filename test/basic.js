'use strict';

const {assert} = require('chai');

const csvBatch = require('../index');
const createStreamFromString = require('./lib/create-stream-from-string');

describe('basic', () => {
  it('empty file', () => {
    return csvBatch(createStreamFromString('')).then(results => {
      assert.equal(results.totalRecords, 0);
      assert.isEmpty(results.data);
      assert.isEmpty(results.errors);
    });
  });

  it('empty string vs null', () => {
    const csv = `a,b,c,d\n"",,"""",\r\n1,2,3,`;
    return csvBatch(createStreamFromString(csv), {nullOnEmpty: true}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          a: '',
          b: null,
          c: '"',
          d: null
        },
        {
          a: '1',
          b: '2',
          c: '3',
          d: null
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('set header', () => {
    const csv = `1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {header: false, columns: ['a', 'b', 'c']}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          a: '1',
          b: '2',
          c: '3'
        },
        {
          a: '4',
          b: '5',
          c: '6'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('drops extra columns if headers', () => {
    const csv = `a,b,c\n1,2,3,4\n5,6,7,8`;
    return csvBatch(createStreamFromString(csv)).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          a: '1',
          b: '2',
          c: '3'
        },
        {
          a: '5',
          b: '6',
          c: '7'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('no header just array of array', () => {
    const csv = `1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {header: false}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        ['1', '2', '3'],
        ['4', '5', '6']
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('columns not an array returns array of array', () => {
    const csv = `1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {header: false, columns: false}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        ['1', '2', '3'],
        ['4', '5', '6']
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('detail turned on', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {detail: true}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          line: 2,
          raw: '1,2,3',
          data: {
            a: '1',
            b: '2',
            c: '3'
          }
        },
        {
          line: 3,
          raw: '4,5,6',
          data: {
            a: '4',
            b: '5',
            c: '6'
          }
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('detail turned on no header', () => {
    const csv = `1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {detail: true, header: false}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          line: 1,
          raw: '1,2,3',
          data: ['1', '2', '3']
        },
        {
          line: 2,
          raw: '4,5,6',
          data: ['4', '5', '6']
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('columns handle header value that is not a string', () => {
    const csv = `4,5,6`;
    return csvBatch(createStreamFromString(csv), {header: false, columns: [1, 2, 3]}).then(results => {
      assert.equal(results.totalRecords, 1);
      assert.deepEqual(results.data, [
        {
          '1': '4',
          '2': '5',
          '3': '6'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('skip header if matches columns', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {header: false, columns: ['a', 'b', 'c']}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          a: '1',
          b: '2',
          c: '3'
        },
        {
          a: '4',
          b: '5',
          c: '6'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('do not skip header if columns does not match first row', () => {
    const csv = `a,b,c,d\n1,2,3`;
    return csvBatch(createStreamFromString(csv), {header: false, columns: ['a', 'b', 'c']}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          a: 'a',
          b: 'b',
          c: 'c'
        },
        {
          a: '1',
          b: '2',
          c: '3'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });
});
