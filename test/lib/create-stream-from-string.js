'use strict';

const {Readable} = require('stream');

/**
 *
 * @param {String} text - string to turn into a stream
 * @return {Object} - a readable stream
 */
function createStreamFromString(text) {
  const stringStream = new Readable({
    read: () => {}
  });
  stringStream.push(text);
  stringStream.push(null);
  return stringStream;
}

module.exports = createStreamFromString;
