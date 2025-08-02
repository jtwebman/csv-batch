# Database Benchmark Implementations

This directory contains modular implementations of CSV parsers for different database scenarios. Each subdirectory represents a specific database configuration, and each file within shows how a particular CSV parser is integrated with that database.

## Structure

```
implementations/
├── sqlite/              # SQLite with standard settings
│   ├── csv-batch.js     # csv-batch implementation
│   ├── csv-parser.js    # csv-parser implementation
│   ├── fast-csv.js      # fast-csv implementation
│   ├── papaparse.js     # papaparse implementation
│   └── csv-parse.js     # csv-parse implementation
├── postgres/            # PostgreSQL with INSERT statements
├── postgres-copy/       # PostgreSQL with COPY protocol
├── mysql/               # MySQL with INSERT statements
├── mysql-load-data/     # MySQL with LOAD DATA INFILE
└── sqlite-optimized/    # SQLite with performance optimizations
```

## Purpose

This organization serves several important purposes:

### 📚 **For Library Authors**
- **See exactly how your parser is tested** - Each file shows the specific implementation used for benchmarking
- **Suggest optimizations** - If you see suboptimal usage patterns, you can propose improvements
- **Understand performance characteristics** - See how your parser behaves with different database scenarios
- **Compare implementations** - See how other parsers solve similar problems

### 🔍 **For Users**
- **Copy proven patterns** - Use these implementations as starting points for your own projects
- **Understand trade-offs** - See how different parsers handle batching, error handling, and memory management
- **Choose the right parser** - Based on your specific database and performance requirements

### 🧪 **For Benchmark Maintainers**
- **Ensure fair testing** - Each parser is tested using its optimal patterns
- **Easy to modify** - Update individual implementations without affecting others
- **Clear documentation** - Each file includes optimization notes and explanations

## Implementation Guidelines

Each implementation file should:

1. **Use realistic patterns** - Implement the parser as you would in a real application
2. **Handle errors gracefully** - Proper error handling and cleanup
3. **Include optimization notes** - Comments explaining choices and potential improvements
4. **Use consistent batch sizes** - For fair comparison across parsers
5. **Follow the parser's idioms** - Use each library's recommended patterns

## Example Implementation

```javascript
/**
 * Example parser implementation for SQLite
 * 
 * Uses the parser's streaming interface with manual batching.
 * Specific optimization notes for library authors.
 */
async function benchmarkParser(csvPath, db, totalRecords) {
  return Profiler.measure('parser-name', totalRecords, async (profiler) => {
    // Implementation using parser's optimal patterns
    // with proper error handling and progress tracking
  });
}
```

## Contributing

When adding a new parser or improving an existing implementation:

1. **Test your changes** - Ensure the implementation works correctly
2. **Document optimizations** - Add comments explaining performance choices
3. **Be fair** - Use each parser's recommended patterns
4. **Consider real-world usage** - Implement patterns you'd actually use in production

## Database-Specific Notes

- **SQLite**: Direct INSERT statements, good for single-threaded scenarios
- **SQLite Optimized**: WAL mode, larger cache, reduced sync for better performance
- **PostgreSQL**: Standard SQL INSERT statements, works with any SQL database
- **PostgreSQL COPY**: Uses PostgreSQL's binary COPY protocol for maximum speed
- **MySQL**: Standard SQL INSERT statements with datetime conversion
- **MySQL LOAD DATA**: Uses MySQL's bulk import command for better performance