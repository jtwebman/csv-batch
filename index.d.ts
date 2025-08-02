/// <reference types="node" />

import { Readable } from 'stream';

/**
 * CSV Security Error - thrown when security limits are exceeded
 */
declare class CsvSecurityError extends Error {
  /** Error name */
  name: 'CsvSecurityError';
  /** Security error code */
  code: 'FIELD_SIZE_LIMIT_EXCEEDED' | 'RECORD_SIZE_LIMIT_EXCEEDED';
  /** Line number where error occurred */
  line: number;
  /** Column number where error occurred */
  column: number;
  
  constructor(message: string, code: string, line: number, column: number);
}

/**
 * Represents a parsed CSV record with detailed information
 */
declare interface DetailedRecord<T = any> {
  /** Line number where the record ended */
  line: number;
  /** Raw string representation of the record */
  raw: string;
  /** Parsed data for the record */
  data: T;
}

/**
 * Enhanced error information for parsing or processing errors
 */
declare interface CsvError {
  /** Line number where error occurred */
  line: number;
  /** Column number where error occurred */
  column: number;
  /** Error category */
  category: 'parsing' | 'mapping' | 'reducing' | 'batching' | 'security';
  /** The original error that occurred */
  error: Error;
  /** User-friendly error message */
  userMessage: string;
  /** Additional context information */
  context: {
    /** Record number being processed */
    recordNumber: number;
    /** Current record data (partial) */
    currentRecord: string[] | null;
    /** Number of fields in current record */
    fieldCount: number;
    /** Last 50 characters of raw content */
    rawContent: string;
    /** Additional category-specific details */
    [key: string]: any;
  };
  /** ISO timestamp when error occurred */
  timestamp: string;
}

/**
 * Results returned by the CSV parser
 */
declare interface CsvBatchResults<T = any> {
  /** Total number of records processed */
  totalRecords: number;
  /** Parsed data or batch results */
  data: T;
  /** Array of errors that occurred during processing */
  errors: CsvError[];
}

/**
 * Map function type - transforms each record
 */
declare type MapFunction<TInput = any, TOutput = any> = (
  record: TInput
) => TOutput | Promise<TOutput> | null | undefined;

/**
 * Batch execution function type - processes each batch
 */
declare type BatchExecutionFunction<TInput = any, TOutput = any> = (
  batch: TInput[]
) => TOutput | Promise<TOutput> | null | undefined;

/**
 * Reducer function type - reduces records into accumulated value
 */
declare type ReducerFunction<TRecord = any, TAccumulator = any> = (
  current: TAccumulator,
  record: TRecord,
  index: number
) => TAccumulator | Promise<TAccumulator>;

/**
 * Function to get initial value for reducer
 */
declare type GetInitialValueFunction<T = any> = () => T;

/**
 * Configuration options for CSV parsing
 */
declare interface CsvBatchOptions<TRecord = any, TBatchResult = any, TReducerResult = any> {
  /**
   * Whether the first row contains headers
   * @default true
   */
  header?: boolean;
  
  /**
   * Column names to use (overrides header row)
   * Can be false to return arrays instead of objects
   * @default []
   */
  columns?: string[] | number[] | false;
  
  /**
   * Character used to separate fields
   * @default ','
   */
  delimiter?: string;
  
  /**
   * Character used for quoting fields
   * @default '"'
   */
  quote?: string;
  
  /**
   * Include detailed information (line number, raw string) with each record
   * @default false
   */
  detail?: boolean;
  
  /**
   * Convert empty fields to null instead of empty string
   * @default false
   */
  nullOnEmpty?: boolean;
  
  /**
   * Function to transform each record
   * @default record => record
   */
  map?: MapFunction<TRecord, TRecord>;
  
  /**
   * Enable batch processing mode
   * @default false
   */
  batch?: boolean;
  
  /**
   * Number of records per batch
   * @default 10000
   */
  batchSize?: number;
  
  /**
   * Function to process each batch
   * @default batch => batch
   */
  batchExecution?: BatchExecutionFunction<TRecord, TBatchResult>;
  
  /**
   * Function to get initial value for reducer
   * @default () => []
   */
  getInitialValue?: GetInitialValueFunction<TReducerResult>;
  
  /**
   * Function to reduce records into accumulated value
   * @default (current, record) => { current.push(record); return current; }
   */
  reducer?: ReducerFunction<TRecord, TReducerResult>;
  
  // Security options
  
  /**
   * Maximum size allowed for a single field (in characters)
   * @default 1048576 (1MB)
   */
  maxFieldSize?: number;
  
  /**
   * Maximum size allowed for a single record (in characters)
   * @default 10485760 (10MB)
   */
  maxRecordSize?: number;
  
  /**
   * Enable CSV injection prevention
   * @default false
   */
  preventCsvInjection?: boolean;
  
  /**
   * Prefixes that indicate potential CSV injection
   * @default ['=', '+', '-', '@', '\t', '\r']
   */
  csvInjectionPrefixes?: string[];
}

/**
 * Parse CSV data from a readable stream with optional batching
 * 
 * @param readStream - A readable stream containing CSV data
 * @param options - Configuration options for parsing
 * @returns Promise that resolves to parsing results
 * @throws {TypeError} When readStream is not a readable stream or options are invalid
 * @throws {RangeError} When option values are out of valid range
 * @throws {CsvSecurityError} When security limits are exceeded
 * 
 * @example
 * // Basic usage
 * const results = await csvBatch(fs.createReadStream('data.csv'));
 * console.log(results.data); // Array of parsed records
 * 
 * @example
 * // Batched processing
 * await csvBatch(stream, {
 *   batch: true,
 *   batchSize: 5000,
 *   batchExecution: async (batch) => {
 *     await database.insertMany(batch);
 *     return batch.length; // Return count of processed records
 *   }
 * });
 * 
 * @example
 * // With custom options
 * const results = await csvBatch(stream, {
 *   delimiter: ';',
 *   quote: "'",
 *   nullOnEmpty: true,
 *   preventCsvInjection: true,
 *   maxFieldSize: 1000,
 *   map: (record) => ({
 *     ...record,
 *     timestamp: new Date()
 *   })
 * });
 */
declare function csvBatch<TRecord = Record<string, string>>(
  readStream: Readable,
  options?: CsvBatchOptions<TRecord>
): Promise<CsvBatchResults<TRecord[]>>;

declare function csvBatch<TRecord = Record<string, string>, TBatchResult = any>(
  readStream: Readable,
  options: CsvBatchOptions<TRecord, TBatchResult> & { batch: true }
): Promise<CsvBatchResults<TBatchResult[]>>;

declare function csvBatch<TRecord = Record<string, string>, TBatchResult = any, TReducerResult = any>(
  readStream: Readable,
  options: CsvBatchOptions<TRecord, TBatchResult, TReducerResult> & { 
    batch: false;
    reducer: ReducerFunction<TRecord, TReducerResult>;
  }
): Promise<CsvBatchResults<TReducerResult>>;

declare namespace csvBatch {
  export {
    CsvSecurityError,
    DetailedRecord,
    CsvError,
    CsvBatchResults,
    MapFunction,
    BatchExecutionFunction,
    ReducerFunction,
    GetInitialValueFunction,
    CsvBatchOptions
  };
}

export = csvBatch;