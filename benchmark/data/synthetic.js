'use strict';

const fs = require('fs');
const path = require('path');

// Based on uDSV's litmus test generation approach
// Generate synthetic datasets with controlled characteristics for peak performance testing

const CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ';

function randStr(length) {
  let s = '';
  for (; s.length < length; s += CHARS.charAt(Math.random() * CHARS.length | 0));
  return s;
}

function randInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1) + min);
}

function generateSyntheticDatasets() {
  const rows = 10000; // 10K rows like uDSV
  const cols = 20;    // 20 columns like uDSV
  const dataDir = path.join(__dirname);

  console.log('Generating synthetic datasets for benchmarking...');

  // 1. litmus_strings.csv - Pure string data (no quotes)
  console.log('- litmus_strings.csv (string data)');
  let out = [];
  for (let i = 0; i < rows; i++) {
    out.push(
      Array.from(
        { length: cols },
        () => randStr(randInt(3, 15))
      ).join(',')
    );
  }
  fs.writeFileSync(path.join(dataDir, 'litmus_strings.csv'), out.join('\n'));

  // 2. litmus_ints.csv - Header + integer data
  console.log('- litmus_ints.csv (header + integers)');
  out = [];
  for (let i = 0; i < rows; i++) {
    out.push(
      Array.from(
        { length: cols },
        () => i === 0 ? randStr(randInt(3, 15)) : randInt(-10000, 10000)
      ).join(',')
    );
  }
  fs.writeFileSync(path.join(dataDir, 'litmus_ints.csv'), out.join('\n'));

  // 3. litmus_quoted.csv - Mixed quoted data
  console.log('- litmus_quoted.csv (quoted strings and numbers)');
  out = [];
  for (let i = 0; i < rows; i++) {
    out.push(
      Array.from(
        { length: cols },
        (v, j) => i === 0 || j % 2 === 0 
          ? `"${randStr(randInt(3, 15))}"` 
          : `"${randInt(-10000, 10000)}"`
      ).join(',')
    );
  }
  fs.writeFileSync(path.join(dataDir, 'litmus_quoted.csv'), out.join('\n'));

  // 4. litmus_mixed.csv - Complex mixed types for stress testing
  console.log('- litmus_mixed.csv (mixed data types with edge cases)');
  out = [];
  for (let i = 0; i < rows; i++) {
    if (i === 0) {
      // Header row
      out.push(Array.from({ length: cols }, (_, j) => `col${j}`).join(','));
    } else {
      const row = Array.from({ length: cols }, (_, j) => {
        switch (j % 6) {
          case 0: return randStr(randInt(5, 20)); // Long strings
          case 1: return randInt(-100000, 100000); // Large integers
          case 2: return `"${randStr(randInt(1, 10))}, with comma"`; // Quoted with comma
          case 3: return `"${randStr(randInt(1, 5))}""quoted""${randStr(randInt(1, 5))}"`; // Escaped quotes
          case 4: return Math.random().toFixed(6); // Floats
          case 5: return ''; // Empty fields
          default: return randStr(5);
        }
      });
      out.push(row.join(','));
    }
  }
  fs.writeFileSync(path.join(dataDir, 'litmus_mixed.csv'), out.join('\n'));

  // 5. litmus_wide.csv - Very wide CSV (100 columns)
  console.log('- litmus_wide.csv (100 columns x 1K rows)');
  const wideCols = 100;
  const wideRows = 1000;
  out = [];
  for (let i = 0; i < wideRows; i++) {
    out.push(
      Array.from(
        { length: wideCols },
        () => i === 0 ? `col${Math.random().toString(36).substr(2, 8)}` : randStr(randInt(2, 8))
      ).join(',')
    );
  }
  fs.writeFileSync(path.join(dataDir, 'litmus_wide.csv'), out.join('\n'));

  // Print file information
  console.log('\nGenerated files:');
  const files = [
    'litmus_strings.csv',
    'litmus_ints.csv', 
    'litmus_quoted.csv',
    'litmus_mixed.csv',
    'litmus_wide.csv'
  ];

  files.forEach(file => {
    const filepath = path.join(dataDir, file);
    const stats = fs.statSync(filepath);
    const sizeMB = (stats.size / 1024 / 1024).toFixed(2);
    console.log(`  ${file}: ${sizeMB} MB`);
  });
}

// Generate CSV files with edge cases for parsing stress tests
function generateEdgeCases() {
  const dataDir = path.join(__dirname);
  
  console.log('\nGenerating edge case datasets...');

  // Edge case: CSV with embedded newlines in quoted fields
  const edgeNewlines = [
    'id,description,notes',
    '1,"Multi\nline\nfield","Simple note"',
    '2,"Another\r\nfield","With CRLF"',
    '3,"Normal field","Normal\nnote"'
  ];
  fs.writeFileSync(path.join(dataDir, 'edge_newlines.csv'), edgeNewlines.join('\n'));

  // Edge case: CSV with various quote escaping
  const edgeQuotes = [
    'id,text,quoted',
    '1,"Simple text","No quotes"',
    '2,"Text with ""quotes""","Embedded ""quotes"""',
    '3,"Text, with comma","Quote with, comma"',
    '4,"","Empty quoted field"'
  ];
  fs.writeFileSync(path.join(dataDir, 'edge_quotes.csv'), edgeQuotes.join('\n'));

  console.log('- edge_newlines.csv (embedded newlines)');
  console.log('- edge_quotes.csv (quote escaping)');
}

if (require.main === module) {
  generateSyntheticDatasets();
  generateEdgeCases();
}

module.exports = {
  generateSyntheticDatasets,
  generateEdgeCases,
  randStr,
  randInt
};