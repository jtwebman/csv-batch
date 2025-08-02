'use strict';

const fs = require('fs');
const path = require('path');
const cliProgress = require('cli-progress');

const departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations', 'Support', 'Product'];
const firstNames = ['John', 'Jane', 'Bob', 'Alice', 'Charlie', 'Emma', 'David', 'Sarah', 'Michael', 'Lisa'];
const lastNames = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez'];

function generateRecord(id) {
  const firstName = firstNames[Math.floor(Math.random() * firstNames.length)];
  const lastName = lastNames[Math.floor(Math.random() * lastNames.length)];
  const age = 22 + Math.floor(Math.random() * 43); // 22-65
  const salary = 40000 + Math.floor(Math.random() * 160000); // 40k-200k
  const department = departments[Math.floor(Math.random() * departments.length)];
  const createdAt = new Date(Date.now() - Math.floor(Math.random() * 365 * 24 * 60 * 60 * 1000)).toISOString();
  
  return {
    id,
    name: `${firstName} ${lastName}`,
    email: `${firstName.toLowerCase()}.${lastName.toLowerCase()}${id}@example.com`,
    age,
    salary,
    department,
    created_at: createdAt
  };
}

function generateCSV(filename, recordCount, options = {}) {
  const {
    includeHeader = true,
    chunkSize = 10000,
    progressBar = true
  } = options;

  console.log(`Generating ${recordCount.toLocaleString()} records to ${filename}...`);
  
  const startTime = Date.now();
  const writeStream = fs.createWriteStream(filename);
  
  let bar;
  if (progressBar) {
    bar = new cliProgress.SingleBar({
      format: 'Progress |{bar}| {percentage}% | {value}/{total} records | ETA: {eta}s',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      hideCursor: true
    });
    bar.start(recordCount, 0);
  }

  // Write header
  if (includeHeader) {
    writeStream.write('id,name,email,age,salary,department,created_at\n');
  }

  let written = 0;
  let buffer = '';

  function writeChunk() {
    const chunk = Math.min(chunkSize, recordCount - written);
    
    for (let i = 0; i < chunk; i++) {
      const record = generateRecord(written + i + 1);
      buffer += `${record.id},"${record.name}",${record.email},${record.age},${record.salary},${record.department},${record.created_at}\n`;
    }
    
    const canContinue = writeStream.write(buffer);
    buffer = '';
    written += chunk;
    
    if (progressBar) {
      bar.update(written);
    }
    
    if (written < recordCount) {
      if (canContinue) {
        setImmediate(writeChunk);
      } else {
        writeStream.once('drain', writeChunk);
      }
    } else {
      writeStream.end();
      if (progressBar) {
        bar.stop();
      }
      
      const elapsed = (Date.now() - startTime) / 1000;
      const size = fs.statSync(filename).size;
      const sizeMB = (size / 1024 / 1024).toFixed(2);
      const rate = (recordCount / elapsed).toFixed(0);
      
      console.log(`\nGenerated ${recordCount.toLocaleString()} records in ${elapsed.toFixed(2)}s`);
      console.log(`File size: ${sizeMB} MB`);
      console.log(`Generation rate: ${parseInt(rate).toLocaleString()} records/second`);
    }
  }

  writeChunk();
}

// Different test file sizes
const fileSizes = {
  small: { records: 100000, file: 'data-100k.csv' },        // ~10 MB
  medium: { records: 1000000, file: 'data-1m.csv' },        // ~100 MB
  large: { records: 10000000, file: 'data-10m.csv' },       // ~1 GB
  xlarge: { records: 100000000, file: 'data-100m.csv' }     // ~10 GB
};

if (require.main === module) {
  const args = process.argv.slice(2);
  const size = args[0] || 'medium';
  
  if (!fileSizes[size]) {
    console.error(`Invalid size. Choose from: ${Object.keys(fileSizes).join(', ')}`);
    process.exit(1);
  }
  
  const dataDir = path.join(__dirname, '..', 'results');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  
  const config = fileSizes[size];
  const filepath = path.join(dataDir, config.file);
  
  generateCSV(filepath, config.records);
}

module.exports = { generateCSV, generateRecord, fileSizes };