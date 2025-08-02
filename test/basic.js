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

  it('can override column headers', () => {
    const csv = `a,b,c\n1,2,3\n4,5,6`;
    return csvBatch(createStreamFromString(csv), {header: true, columns: ['col1', 'col2', 'col3']}).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          col1: '1',
          col2: '2',
          col3: '3'
        },
        {
          col1: '4',
          col2: '5',
          col3: '6'
        }
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
          1: '4',
          2: '5',
          3: '6'
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

  describe('parsing edge cases', () => {
    it('handles empty CSV values with nullOnEmpty option', () => {
      const csv = 'a,b\n,empty\nempty,';
      return csvBatch(createStreamFromString(csv), {
        nullOnEmpty: true
      }).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.equal(results.data[0].a, null);
        assert.equal(results.data[0].b, 'empty');
        assert.equal(results.data[1].a, 'empty');
        assert.equal(results.data[1].b, null);
      });
    });

    it('handles quoted empty values correctly', () => {
      const csv = 'a,b\n"","value"\n"value",""';
      return csvBatch(createStreamFromString(csv), {
        nullOnEmpty: true
      }).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.equal(results.data[0].a, ''); // Quoted empty is not null
        assert.equal(results.data[0].b, 'value');
        assert.equal(results.data[1].a, 'value');
        assert.equal(results.data[1].b, ''); // Quoted empty is not null
      });
    });

    it('handles CSV with only commas', () => {
      const csv = ',,,\n,,,';
      return csvBatch(createStreamFromString(csv), {
        header: false,
        nullOnEmpty: true
      }).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.deepEqual(results.data[0], [null, null, null, null]);
        assert.deepEqual(results.data[1], [null, null, null, null]);
      });
    });

    it('handles consecutive quote sequences', () => {
      const csv = 'field\n""""'; // Four quotes should become one quote
      return csvBatch(createStreamFromString(csv)).then(results => {
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0].field, '"');
      });
    });

    it('handles final record processing without newline', () => {
      const csv = 'a,b\n1,2\n3,4'; // No final newline
      return csvBatch(createStreamFromString(csv)).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.equal(results.data[1].a, '3');
        assert.equal(results.data[1].b, '4');
      });
    });

    it('handles records with more fields than columns', () => {
      const csv = 'a,b\n1,2,3,4'; // More fields than columns
      return csvBatch(createStreamFromString(csv), {
        columns: ['first', 'second'] // Only 2 columns defined
      }).then(results => {
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0].first, '1');
        assert.equal(results.data[0].second, '2');
        assert.isUndefined(results.data[0].third); // Extra fields ignored
      });
    });

    it('handles makeRecord with empty columns array', () => {
      const csv = 'a,b\n1,2';
      return csvBatch(createStreamFromString(csv), {
        header: false,
        columns: [] // Empty columns array
      }).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.deepEqual(results.data[0], ['a', 'b']); // Returns array since no column mapping
        assert.deepEqual(results.data[1], ['1', '2']);
      });
    });

    it('handles non-string column names', () => {
      const csv = 'a,b\n1,2\n3,4';
      return csvBatch(createStreamFromString(csv), {
        columns: [123, 456] // Non-string column names
      }).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.equal(results.data[0]['123'], '1');
        assert.equal(results.data[0]['456'], '2');
      });
    });

    it('handles case-insensitive header matching edge cases', () => {
      const csv = 'Name,Age\nname,age\nJohn,25';
      return csvBatch(createStreamFromString(csv), {
        header: false,
        columns: ['Name', 'Age'] // Should detect first row as header
      }).then(results => {
        assert.equal(results.totalRecords, 2);
        assert.equal(results.data[0].Name, 'name');
        assert.equal(results.data[1].Name, 'John');
      });
    });

    it('handles columns array length mismatch', () => {
      const csv = 'a,b,c,d\n1,2'; // Record has fewer fields than header
      return csvBatch(createStreamFromString(csv), {
        columns: ['first', 'second', 'third', 'fourth']
      }).then(results => {
        assert.equal(results.totalRecords, 1);
        assert.equal(results.data[0].first, '1');
        assert.equal(results.data[0].second, '2');
        assert.isUndefined(results.data[0].third);
        assert.isUndefined(results.data[0].fourth);
      });
    });

    it('handles header detection with number columns', () => {
      const csv = '1,2\n1,2\n3,4';
      return csvBatch(createStreamFromString(csv), {
        header: false,
        columns: [1, 2] // Number columns that match first row
      }).then(results => {
        // Should NOT detect first row as header since numbers don't match strings
        assert.equal(results.totalRecords, 3);
        assert.equal(results.data[0]['1'], '1');
        assert.equal(results.data[1]['1'], '1');
        assert.equal(results.data[2]['1'], '3');
      });
    });
  });
});
