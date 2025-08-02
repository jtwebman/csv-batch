# CSV-Batch: Enterprise-Grade CSV Parser for Node.js

[![Build Status](https://travis-ci.org/jtwebman/csv-batch.svg?branch=master)](https://travis-ci.org/jtwebman/csv-batch)
[![Coverage Status](https://coveralls.io/repos/github/jtwebman/csv-batch/badge.svg?branch=master)](https://coveralls.io/github/jtwebman/csv-batch?branch=master)
[![MIT Licence](https://badges.frapsoft.com/os/mit/mit.svg?v=103)](https://opensource.org/licenses/mit-license.php)
[![node](https://img.shields.io/node/v/csv-batch.svg)](https://www.npmjs.org/package/csv-batch)
[![Known Vulnerabilities](https://snyk.io/test/github/jtwebman/csv-batch/badge.svg)](https://snyk.io/test/github/jtwebman/csv-batch)

A high-performance, memory-efficient CSV parser designed for production workloads. Built with **zero dependencies**, comprehensive **TypeScript support**, and enterprise-grade **security features**.

## ✨ Key Features

- 🚀 **Ultra-fast**: Processes 1M+ records in ~1.5 seconds
- 💧 **Memory-efficient**: Stream-based processing with configurable batching
- 🔒 **Security-first**: Built-in CSV injection protection and size limits
- 📘 **TypeScript**: Full type definitions with generics support
- 🛡️ **Robust**: 95%+ test coverage with comprehensive error handling
- 🔧 **Flexible**: Supports mapping, reducing, and custom processing
- 📊 **Production-ready**: Enhanced error reporting with context

## 📦 Installation

```bash
npm install csv-batch
```

## 🚀 Quick Start

```javascript
const csvBatch = require('csv-batch');
const fs = require('fs');

// Basic usage - parse entire file to memory
const results = await csvBatch(fs.createReadStream('data.csv'));
console.log(`Processed ${results.totalRecords} records`);
console.log(results.data); // Array of parsed records
```

## 📚 Usage Examples

### Memory-Efficient Batch Processing

Perfect for processing large files without loading everything into memory:

```javascript
const csvBatch = require('csv-batch');

const results = await csvBatch(fileStream, {
  batch: true,
  batchSize: 10000,
  batchExecution: async (batch) => {
    // Process each batch (e.g., insert to database)
    await database.insertMany(batch);
    console.log(`Processed batch of ${batch.length} records`);
  }
});

console.log(`Total processed: ${results.totalRecords} records`);
```

### Data Transformation & Mapping

Transform records during parsing:

```javascript
const results = await csvBatch(fileStream, {
  map: async (record) => {
    // Transform each record
    return {
      id: parseInt(record.id),
      email: record.email.toLowerCase(),
      createdAt: new Date(record.created_at),
      // Skip invalid records
      ...(record.email.includes('@') ? {} : null)
    };
  }
});
```

### Data Aggregation & Reducing

Perform real-time aggregations without storing individual records:

```javascript
const results = await csvBatch(fileStream, {
  getInitialValue: () => ({ totalSales: 0, productCounts: {} }),
  reducer: (accumulator, record) => {
    accumulator.totalSales += parseFloat(record.amount);
    accumulator.productCounts[record.product] = 
      (accumulator.productCounts[record.product] || 0) + 1;
    return accumulator;
  }
});

console.log('Aggregated results:', results.data);
```

### Advanced Security & Validation

```javascript
const results = await csvBatch(fileStream, {
  // Security features
  maxFieldSize: 1024 * 100,      // 100KB per field
  maxRecordSize: 1024 * 1000,    // 1MB per record
  preventCsvInjection: true,      // Sanitize dangerous formulas
  
  // Custom validation
  map: (record) => {
    if (!record.email || !record.email.includes('@')) {
      throw new Error(`Invalid email: ${record.email}`);
    }
    return record;
  }
});

// Access detailed error information
results.errors.forEach(error => {
  console.error(`Line ${error.line}: ${error.userMessage}`);
  console.error('Context:', error.context);
});
```

## 🔧 Configuration Options

### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `header` | `boolean` | `true` | Use first row as column headers |
| `columns` | `string[]` | `[]` | Custom column names (overrides header detection) |
| `delimiter` | `string` | `','` | Field separator character |
| `quote` | `string` | `'"'` | Quote character for escaping |
| `detail` | `boolean` | `false` | Include line numbers and raw data in results |
| `nullOnEmpty` | `boolean` | `false` | Convert empty fields to `null` instead of `""` |

### Processing Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `map` | `Function` | `record => record` | Transform each record (supports async) |
| `batch` | `boolean` | `false` | Enable batch processing mode |
| `batchSize` | `number` | `10000` | Records per batch |
| `batchExecution` | `Function` | - | Process each batch (supports async) |
| `getInitialValue` | `Function` | `() => []` | Initial value for reduction |
| `reducer` | `Function` | - | Reduce function for aggregations |

### Security Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxFieldSize` | `number` | `Infinity` | Maximum bytes per field |
| `maxRecordSize` | `number` | `Infinity` | Maximum bytes per record |
| `preventCsvInjection` | `boolean` | `false` | Sanitize formula injections |
| `csvInjectionPrefixes` | `string[]` | `['=', '+', '-', '@']` | Characters to sanitize |

## 📊 Return Value Structure

```typescript
interface CsvResults<T = any> {
  totalRecords: number;    // Successfully processed records
  data: T;                 // Parsed data (array or reduced value)
  errors: CsvError[];      // Detailed error information
}

interface CsvError {
  line: number;           // Line number where error occurred
  column: number;         // Column index
  category: string;       // Error type: 'parsing', 'mapping', 'reducing', 'batching'
  error: Error;          // Original error object
  userMessage: string;   // Human-readable description
  timestamp: string;     // ISO timestamp
  context: {            // Additional debugging context
    recordNumber: number;
    fieldCount: number;
    currentRecord: any[];
    rawContent: string;
  };
}
```

## 🔒 Security Features

### CSV Injection Protection

Automatically sanitizes dangerous formulas that could execute in spreadsheet applications:

```javascript
// Input:  =SUM(1+1)
// Output: '=SUM(1+1)  (prefixed with quote to neutralize)

const results = await csvBatch(fileStream, {
  preventCsvInjection: true,
  csvInjectionPrefixes: ['=', '+', '-', '@', '\t', '\r'] // Customizable
});
```

### Resource Limits

Prevent denial-of-service attacks with configurable limits:

```javascript
const results = await csvBatch(fileStream, {
  maxFieldSize: 1024 * 100,    // 100KB per field
  maxRecordSize: 1024 * 1000,  // 1MB per record
  batchSize: 5000              // Smaller batches for memory control
});
```

## 🏗️ TypeScript Support

Full TypeScript definitions with generic support:

```typescript
import csvBatch from 'csv-batch';

interface UserRecord {
  id: number;
  name: string;
  email: string;
}

// Type-safe parsing
const results = await csvBatch<UserRecord[]>(fileStream, {
  map: (record): UserRecord => ({
    id: parseInt(record.id),
    name: record.name,
    email: record.email.toLowerCase()
  })
});

// results.data is typed as UserRecord[]
results.data.forEach(user => {
  console.log(user.email); // TypeScript knows this is a string
});
```

## ⚡ Performance Characteristics

- **Memory usage**: Constant memory usage with batching enabled
- **Processing speed**: ~650,000 records/second on modern hardware
- **File size**: No practical limit when using batch processing
- **Concurrency**: Single-threaded but non-blocking with async/await

### Benchmarks

| File Size | Records | Memory Usage | Processing Time |
|-----------|---------|--------------|-----------------|
| 100MB | 1M records | ~50MB (batched) | ~1.5 seconds |
| 1GB | 10M records | ~50MB (batched) | ~15 seconds |
| 10GB | 100M records | ~50MB (batched) | ~2.5 minutes |

## 🛠️ Error Handling Best Practices

### Comprehensive Error Handling

```javascript
try {
  const results = await csvBatch(fileStream, {
    map: (record) => {
      // Your processing logic
      if (!record.required_field) {
        throw new Error('Missing required field');
      }
      return record;
    }
  });

  // Process successful records
  console.log(`Successfully processed: ${results.totalRecords}`);
  
  // Handle processing errors (non-fatal)
  if (results.errors.length > 0) {
    console.log(`Encountered ${results.errors.length} errors:`);
    results.errors.forEach(error => {
      console.error(`Line ${error.line}: ${error.userMessage}`);
      
      // Access detailed context for debugging
      if (error.context) {
        console.error('Record data:', error.context.currentRecord);
        console.error('Raw content:', error.context.rawContent);
      }
    });
  }

} catch (error) {
  // Handle fatal errors (invalid stream, security violations, etc.)
  console.error('Fatal error:', error.message);
  
  if (error.name === 'CsvSecurityError') {
    console.error(`Security violation at line ${error.line}`);
  }
}
```

## 🔍 Troubleshooting

### Common Issues

**Memory Issues with Large Files**
```javascript
// ❌ Wrong - loads entire file into memory
const results = await csvBatch(fileStream);

// ✅ Correct - uses constant memory
const results = await csvBatch(fileStream, {
  batch: true,
  batchSize: 1000,
  batchExecution: async (batch) => {
    await processRecords(batch);
  }
});
```

**Performance Optimization**
```javascript
// ✅ Optimized configuration for large files
const results = await csvBatch(fileStream, {
  batch: true,
  batchSize: 10000,        // Larger batches = better throughput
  maxFieldSize: 1024,      // Reasonable limits prevent abuse
  preventCsvInjection: false, // Disable if not needed for performance
});
```

**Handling Malformed CSV**
```javascript
const results = await csvBatch(fileStream, {
  map: (record, index) => {
    try {
      // Validate and transform record
      return validateRecord(record);
    } catch (validationError) {
      // Log but don't fail entire process
      console.warn(`Invalid record at line ${index + 2}:`, validationError.message);
      return null; // Skip this record
    }
  }
});
```

## 📊 Comprehensive Benchmarks

Our benchmark suite compares csv-batch against all major CSV parsing libraries across synthetic parsing tests and real-world database import scenarios.

### Benchmark Types

1. **Synthetic Parsing**: uDSV-style tests focusing on pure parsing performance (string → objects)
2. **Database Imports**: Real-world scenarios importing CSV data into SQLite, PostgreSQL, and MySQL

### Running Benchmarks

```bash
# Run all benchmarks
npm run benchmark

# Run specific benchmark types
npm run benchmark parsing      # Only parsing tests
npm run benchmark database     # Only database tests
npm run benchmark sqlite large # SQLite with large dataset

# Available dataset sizes
npm run benchmark all small    # 100K records (~10 MB)
npm run benchmark all medium   # 1M records (~100 MB)
npm run benchmark all large    # 10M records (~1 GB)
npm run benchmark all xlarge   # 100M records (~10 GB)
```

### Scoring System

Our summary uses a weighted scoring system to rank parser performance across all tests:

- **1st place**: 10 points
- **2nd place**: 8 points  
- **3rd place**: 6 points
- **4th place**: 4 points
- **5th place**: 2 points
- **6th+ place**: 0 points

This ensures that consistent top-5 performance is rewarded over occasional wins.

### Benchmark Results

View the latest comprehensive benchmark results:

- 📊 **[Latest Results](./benchmark/results/README.md)** - Complete benchmark reports with system information
- 🏆 **[All Benchmarks - Medium Dataset](./benchmark/results/benchmark-results-all-medium-latest.md)** - Parsing + Database tests
- ⚡ **[Parsing Only - Medium Dataset](./benchmark/results/benchmark-results-parsing-medium-latest.md)** - Pure parsing performance
- 🗄️ **[Database Only - Medium Dataset](./benchmark/results/benchmark-results-database-medium-latest.md)** - Database import scenarios

The summary report shows:
- **Tests**: Number of benchmark tests each parser completed
- **Avg Rows/sec**: Average parsing throughput across all tests
- **Avg MB/sec**: Average data processing throughput
- **1st/2nd/3rd/4th/5th**: Count of top-5 placements
- **Score**: Total weighted score based on placements

All results include detailed system information (CPU, memory, OS) for reproducibility and context.

### Database Benchmark Features

- **PostgreSQL**: Tests both standard INSERTs and COPY protocol
- **SQLite**: Optimized with WAL mode and performance pragmas
- **MySQL**: Includes LOAD DATA INFILE for bulk imports
- **Memory Monitoring**: Tracks peak memory usage during processing
- **Streaming Focus**: Only includes parsers that support streaming for realistic large-file scenarios

### Modular Implementation

Each database test is organized into modular implementation files:
```
benchmark/database/implementations/
├── postgres/
│   ├── csv-batch.js
│   ├── csv-parser.js
│   └── fast-csv.js
├── sqlite/
└── mysql/
```

This allows library authors to easily see and optimize their implementations for specific database scenarios.

## 🔄 Migration Guide

### From v1.x to v2.x

- ✅ All existing APIs remain backward compatible
- ➕ New security options are opt-in
- ➕ Enhanced error reporting provides more context
- ➕ TypeScript definitions now included

### From other CSV libraries

```javascript
// csv-parser migration
const csv = require('csv-parser');
const csvBatch = require('csv-batch');

// Before
fs.createReadStream('data.csv')
  .pipe(csv())
  .on('data', (data) => console.log(data))
  .on('end', () => console.log('Done'));

// After  
const results = await csvBatch(fs.createReadStream('data.csv'), {
  map: (record) => {
    console.log(record);
    return record;
  }
});
console.log('Done');
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## 📄 License

MIT License - see [LICENSE.md](LICENSE.md) for details.

## 🆘 Support

- 📖 [Documentation](https://github.com/jtwebman/csv-batch/wiki)
- 🐛 [Issues](https://github.com/jtwebman/csv-batch/issues)
- 💬 [Discussions](https://github.com/jtwebman/csv-batch/discussions)

---

**Built with ❤️ for the Node.js community**