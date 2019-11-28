# CSV Parser with Batching

This is a very fast CSV parser with batching for Node.js. It has no dependencies and is returns a promise and functions support promises and async functions so no need to learn streams!

All it returns is a single function that takes a readable Node.js stream like a file stream and options and then resolves once parsed or
allows you to batch records and call a function for each batch. It will wait for the batch function to return resolved before moving on
so you will not waster memory loading the whole CSV in-memory.

If you don't turn on batching then it works like most other csv parsers and does it all in memory.

## Install

```
npm install csv-batch
```

## Usage

### Batching

```
const csvBatch = require('csv-batch');

csvBatch(fileStream, {
  batch: true,
  batchSize: 10000,
  batchExecution: batch => addToDatabase(batch)
}).then(results => {
  console.log(`Processed ${results.totalRecords});
});
```

### In-Memory Results

```
const csvBatch = require('csv-batch');

csvBatch(fileStream).then(results => {
  console.log(`Processed ${results.totalRecords});
  console.log(`CSV as JSON ${JSON.stringify(results.data, null, 2)});
});
```

## Options

- `header: {boolean} = true`: When set to true will take the first column as a header and use them for the object proprty names for each record. If set to false and `columns` option isn't set each record will just be an array.

- `columns: {Array.<String>} = []`: When set to an array of column names will use these columns when parsing the file and creating record objects. If the first line of the file matches these it will skip it but the headers are not required to be there.

- `delimiter: {string} = ','`: This is the character you use to delimit a new column in the csv. **This will always need to be one character only!**

- `quote: {string} = '"'`: This is the character you use to go in and out of quote mode where new lines and delimiter is ignored. If in quote mode to display this character you need to repeat it twice. **This will always need to be one character only!**

- `detail: {boolean} = false`: When set to true each record isn't the parsed data but a object with the line number it ended on, the raw string for the record, and a data property with the object or array of the record.

  - **Example:**

  ```
  {
    line: 2,
    raw: '1,2,3',
    data: {
      a: '1',
      b: '2',
      c: '3'
    }
  }
  ```

- `nullOnEmpty: {boolean} = false`: When set to true if the field is empty and didn't have a empty quotes `""` then the field will be set to null. If set to false will always be a empty string.

- `transform: {Function} = record => record`: When set will be called for each record and will make the record what ever is returned. This will wait for this to return before continueing to parse and supports promises and async functions.

- `batch: {boolean} = false`: When set to true will turn on batch mode and will call the batch execution function for each batch waiting for it to finish to continue parsings.

- `batchSize: {Number} = 10000`: The number of records to include into each batch when running in batch mode.

- `batchExecution: {Function} = batch => batch`: The function that is called for each batch that supports promises and async functions. The csv parser will wait for each batch to finish before moving on in parsing to not have to load the whole file in memory.
