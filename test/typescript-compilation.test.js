'use strict';

const {assert} = require('chai');
const {execSync} = require('child_process');
const fs = require('fs');
const path = require('path');

describe('TypeScript type definitions', () => {
  it('should compile TypeScript test file without errors', () => {
    try {
      // Compile the TypeScript test file
      const result = execSync('npx tsc --noEmit --strict test/typescript-simple.test.ts', {
        encoding: 'utf8',
        cwd: process.cwd()
      });
      
      // If we get here, compilation succeeded
      assert.isString(result);
    } catch (error) {
      // If compilation fails, the error will contain TypeScript errors
      console.error('TypeScript compilation failed:');
      console.error(error.stdout || error.message);
      throw new Error(`TypeScript compilation failed: ${error.stdout || error.message}`);
    }
  });

  it('should have valid index.d.ts file', () => {
    const typesFile = path.join(process.cwd(), 'index.d.ts');
    assert.isTrue(fs.existsSync(typesFile), 'index.d.ts file should exist');
    
    const content = fs.readFileSync(typesFile, 'utf8');
    assert.include(content, 'declare function csvBatch', 'Should export main function');
    assert.include(content, 'CsvBatchOptions', 'Should export options interface');
    assert.include(content, 'CsvBatchResults', 'Should export results interface');
    assert.include(content, 'CsvError', 'Should export error interface');
    assert.include(content, 'CsvSecurityError', 'Should export security error class');
  });

  it('should have correct package.json types field', () => {
    const packageJson = require('../package.json');
    assert.equal(packageJson.types, 'index.d.ts', 'package.json should point to index.d.ts');
  });

  it('should validate type definitions with tsc', () => {
    try {
      // Just validate the type definitions file itself
      const result = execSync('npx tsc --noEmit --strict index.d.ts', {
        encoding: 'utf8',
        cwd: process.cwd()
      });
      
      assert.isString(result);
    } catch (error) {
      console.error('Type definitions validation failed:');
      console.error(error.stdout || error.message);
      throw new Error(`Type definitions invalid: ${error.stdout || error.message}`);
    }
  });

  it('should allow importing types in TypeScript', () => {
    // Create a simple test file that imports our types
    const testCode = `
import csvBatch = require('./index');
import { CsvBatchOptions, CsvBatchResults, CsvError } from './index';

// Test that types can be used
const options: CsvBatchOptions = {
  header: true,
  delimiter: ',',
  batch: false
};

// Test function signature
import { Readable } from 'stream';
declare const stream: Readable;
const promise: Promise<CsvBatchResults> = csvBatch(stream, options);
`;

    const testFile = path.join(process.cwd(), 'temp-type-test.ts');
    fs.writeFileSync(testFile, testCode);

    try {
      execSync(`npx tsc --noEmit --strict ${testFile}`, {
        encoding: 'utf8',
        cwd: process.cwd()
      });
    } catch (error) {
      console.error('Type import test failed:');
      console.error(error.stdout || error.message);
      throw new Error(`Type import failed: ${error.stdout || error.message}`);
    } finally {
      // Clean up
      if (fs.existsSync(testFile)) {
        fs.unlinkSync(testFile);
      }
    }
  });
});