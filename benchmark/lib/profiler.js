'use strict';

const cliProgress = require('cli-progress');

class Profiler {
  constructor(name, totalRecords) {
    this.name = name;
    this.totalRecords = totalRecords;
    this.startTime = null;
    this.endTime = null;
    this.peakMemory = 0;
    this.startMemory = 0;
    this.startRSS = 0;
    this.peakRSS = 0;
    this.endRSS = 0;
    this.gcCount = 0;
    this.recordsProcessed = 0;
    this.errors = 0;
    this.progressBar = null;
    this.memoryInterval = null;
    this.gcWatcher = null;
  }

  start() {
    this.startTime = process.hrtime.bigint();
    const memUsage = process.memoryUsage();
    this.startMemory = memUsage.heapUsed;
    this.startRSS = memUsage.rss;
    this.peakRSS = this.startRSS;
    
    // Create progress bar
    this.progressBar = new cliProgress.SingleBar({
      format: `${this.name} |{bar}| {percentage}% | {value}/{total} records | {rate} rec/s | ETA: {eta}s`,
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      hideCursor: true
    });
    this.progressBar.start(this.totalRecords, 0);
    
    // Monitor memory usage (both heap and RSS)
    this.memoryInterval = setInterval(() => {
      const currentMemUsage = process.memoryUsage();
      const currentMemory = currentMemUsage.heapUsed;
      const currentRSS = currentMemUsage.rss;
      
      if (currentMemory > this.peakMemory) {
        this.peakMemory = currentMemory;
      }
      if (currentRSS > this.peakRSS) {
        this.peakRSS = currentRSS;
      }
    }, 10); // More frequent sampling like uDSV
    
    // Monitor garbage collection
    if (global.gc) {
      const originalGc = global.gc;
      global.gc = (...args) => {
        this.gcCount++;
        return originalGc.apply(this, args);
      };
    }
  }

  update(processed) {
    this.recordsProcessed = processed;
    if (this.progressBar) {
      this.progressBar.update(processed);
    }
  }

  incrementError() {
    this.errors++;
  }

  stop() {
    this.endTime = process.hrtime.bigint();
    
    if (this.progressBar) {
      this.progressBar.stop();
    }
    
    if (this.memoryInterval) {
      clearInterval(this.memoryInterval);
    }
    
    const results = this.getResults();
    this.printResults(results);
    
    return results;
  }

  getResults() {
    const durationNs = this.endTime - this.startTime;
    const durationMs = Number(durationNs) / 1000000;
    const durationSec = durationMs / 1000;
    
    const currentMemUsage = process.memoryUsage();
    const currentMemory = currentMemUsage.heapUsed;
    const currentRSS = currentMemUsage.rss;
    this.endRSS = currentRSS;
    
    // Use peak memory for delta to handle GC cleanup issues
    const memoryDelta = this.peakMemory > 0 ? this.peakMemory - this.startMemory : currentMemory - this.startMemory;
    const rssDelta = this.peakRSS - this.startRSS;
    
    return {
      name: this.name,
      totalRecords: this.totalRecords,
      recordsProcessed: this.recordsProcessed,
      errors: this.errors,
      duration: {
        ms: durationMs,
        seconds: durationSec,
        formatted: this.formatDuration(durationMs)
      },
      throughput: {
        recordsPerSecond: Math.round(this.recordsProcessed / durationSec),
        mbPerSecond: 0 // Will be calculated if file size provided
      },
      memory: {
        heap: {
          start: this.startMemory,
          peak: this.peakMemory,
          final: currentMemory,
          delta: memoryDelta,
          peakDelta: this.peakMemory - this.startMemory,
          formatted: {
            start: this.formatBytes(this.startMemory),
            peak: this.formatBytes(this.peakMemory),
            final: this.formatBytes(currentMemory),
            delta: this.formatBytes(memoryDelta),
            peakDelta: this.formatBytes(this.peakMemory - this.startMemory)
          }
        },
        rss: {
          start: this.startRSS,
          peak: this.peakRSS, 
          final: currentRSS,
          delta: rssDelta,
          peakDelta: this.peakRSS - this.startRSS,
          formatted: {
            start: this.formatBytes(this.startRSS),
            peak: this.formatBytes(this.peakRSS),
            final: this.formatBytes(currentRSS),
            delta: this.formatBytes(rssDelta),
            peakDelta: this.formatBytes(this.peakRSS - this.startRSS)
          }
        }
      },
      gcCount: this.gcCount
    };
  }

  printResults(results) {
    console.log('\n' + '='.repeat(60));
    console.log(`Benchmark: ${results.name}`);
    console.log('='.repeat(60));
    console.log(`Records processed: ${results.recordsProcessed.toLocaleString()} / ${results.totalRecords.toLocaleString()}`);
    console.log(`Errors: ${results.errors}`);
    console.log(`Duration: ${results.duration.formatted}`);
    console.log(`Throughput: ${results.throughput.recordsPerSecond.toLocaleString()} records/second`);
    console.log(`Heap Memory - Peak: ${results.memory.heap.formatted.peak}, Delta: ${results.memory.heap.formatted.peakDelta}`);
    console.log(`RSS Memory - Peak: ${results.memory.rss.formatted.peak}, Delta: ${results.memory.rss.formatted.peakDelta}`);
    if (results.gcCount > 0) {
      console.log(`GC runs: ${results.gcCount}`);
    }
    console.log('='.repeat(60) + '\n');
  }

  formatDuration(ms) {
    if (ms < 1000) {
      return `${ms.toFixed(2)}ms`;
    } else if (ms < 60000) {
      return `${(ms / 1000).toFixed(2)}s`;
    } else {
      const minutes = Math.floor(ms / 60000);
      const seconds = ((ms % 60000) / 1000).toFixed(0);
      return `${minutes}m ${seconds}s`;
    }
  }

  formatBytes(bytes) {
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }
    
    return `${size.toFixed(2)} ${units[unitIndex]}`;
  }

  static async measure(name, totalRecords, fn) {
    const profiler = new Profiler(name, totalRecords);
    profiler.start();
    
    try {
      await fn(profiler);
    } catch (error) {
      profiler.incrementError();
      throw error;
    } finally {
      return profiler.stop();
    }
  }
}

module.exports = Profiler;