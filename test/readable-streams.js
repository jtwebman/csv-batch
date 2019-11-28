'use strict';

const {assert} = require('chai');
const fs = require('fs');
const zlib = require('zlib');
const path = require('path');
const http = require('http');

const csvBatch = require('../index');

describe('readable stream', () => {
  it('file system', () => {
    const fileStream = fs.createReadStream(path.join(__dirname, 'files', 'readable-stream-fs.csv'));
    return csvBatch(fileStream).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          A: '1',
          B: '2',
          C: '3'
        },
        {
          A: '4',
          B: '5',
          C: '6'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('zlib', () => {
    const zlibStream = fs.createReadStream(path.join(__dirname, 'files', 'readable-stream-zlib.csv.gz'));
    const unzip = zlib.createGunzip();
    return csvBatch(zlibStream.pipe(unzip)).then(results => {
      assert.equal(results.totalRecords, 2);
      assert.deepEqual(results.data, [
        {
          A: '1',
          B: '2',
          C: '3'
        },
        {
          A: '4',
          B: '5',
          C: '6'
        }
      ]);
      assert.isEmpty(results.errors);
    });
  });

  it('http', () => {
    return new Promise(resolve => {
      http.get('http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv', res => {
        resolve(res);
      });
    })
      .then(csvBatch)
      .then(results => {
        assert.equal(results.totalRecords, 985);
        assert.equal(results.data.length, 985);
        assert.isEmpty(results.errors);
      });
  });
});
