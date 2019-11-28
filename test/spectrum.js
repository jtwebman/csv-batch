/* eslint-disable require-jsdoc */
'use strict';

const spectrum = require('csv-spectrum');
const {Readable} = require('stream');
const {StringDecoder} = require('string_decoder');
const {assert} = require('chai');

const csvBatch = require('../index');
const decoder = new StringDecoder();

function getTests() {
  return new Promise((resolve, reject) => {
    spectrum((err, tests) => {
      if (err) {
        return reject(err);
      }
      const testsWithStreams = tests.map(test => {
        const csvStream = new Readable();
        csvStream._read = () => {};
        csvStream.push(test.csv);
        csvStream.push(null);

        return {
          name: test.name,
          json: JSON.parse(decoder.write(test.json)),
          csv: csvStream
        };
      });
      resolve(testsWithStreams);
    });
  });
}

describe('csv spectrum', () => {
  it('all tests', () => {
    return getTests().then(tests => {
      const testTasks = tests.map(test => {
        return csvBatch(test.csv).then(results => {
          assert.deepEqual(results.data, test.json, `${test.name} did not match`);
        });
      });
      return Promise.all(testTasks);
    });
  });
});
