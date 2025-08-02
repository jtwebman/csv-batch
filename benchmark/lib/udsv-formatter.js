'use strict';

// uDSV-style result formatter with ASCII tables and performance bars
// Based on the excellent presentation format from uDSV benchmarks

class UDSVFormatter {
  static formatNumber(num, precision = 3) {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    } else {
      return num.toFixed(precision > 0 ? 1 : 0);
    }
  }

  static formatBytes(bytes, precision = 1) {
    if (bytes === 0) return '0 B';
    
    const k = 1024;
    const sizes = ['B', 'KiB', 'MiB', 'GiB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(precision))} ${sizes[i]}`;
  }

  static createProgressBar(percentage, width, char = '░') {
    const filled = Math.ceil(width * percentage);
    return char.repeat(filled);
  }

  // Create uDSV-style table with performance bars
  static formatBenchmarkTable(results, fileInfo) {
    const validResults = results.filter(r => !r.error);
    if (validResults.length === 0) {
      return 'No successful benchmarks to display.';
    }

    // Find max values for scaling bars
    const maxRowsPerSec = Math.max(...validResults.map(r => r.rowsPerSec || 0));
    const maxMbPerSec = Math.max(...validResults.map(r => r.mbPerSec || 0));
    const maxMemoryDelta = Math.max(...validResults.map(r => 
      r.memoryDelta || r.memory?.rss?.peakDelta || 0));

    // Column widths
    const nameWidth = 24;
    const rowsWidth = 8;
    const throughputWidth = 60;
    const memoryWidth = 33;
    const typesWidth = 8;
    const sampleWidth = 52;

    // Create header
    const dataHeader = `${fileInfo.file} (${this.formatBytes(fileInfo.fileSizeMB * 1024 * 1024, 1)}, ${this.formatNumber(fileInfo.cols || 20, 0)} cols x ${this.formatNumber(fileInfo.rowCount, 0)} rows)`;
    const throughputHeader = 'Throughput (MiB/s)';
    const memoryHeader = `RSS above baseline (MiB)`;

    let output = [];
    
    // Top border
    output.push('┌' + '─'.repeat(nameWidth + rowsWidth + throughputWidth + memoryWidth + typesWidth + sampleWidth + 10) + '┐');
    
    // File info header
    const headerPadding = Math.max(0, nameWidth + rowsWidth + throughputWidth + memoryWidth + typesWidth + sampleWidth + 10 - dataHeader.length);
    output.push(`│ ${dataHeader}${' '.repeat(headerPadding - 1)}│`);
    
    // Column headers
    output.push('├' + '─'.repeat(nameWidth) + '┬' + '─'.repeat(rowsWidth) + '┬' + 
                '─'.repeat(throughputWidth) + '┬' + '─'.repeat(memoryWidth) + '┬' +
                '─'.repeat(typesWidth) + '┬' + '─'.repeat(sampleWidth) + '┤');
    
    const headerRow = `│ ${'Name'.padEnd(nameWidth - 1)}│ ${'Rows/s'.padEnd(rowsWidth - 1)}│ ${throughputHeader.padEnd(throughputWidth - 1)}│ ${memoryHeader.padEnd(memoryWidth - 1)}│ ${'Types'.padEnd(typesWidth - 1)}│ ${'Sample'.padEnd(sampleWidth - 1)}│`;
    output.push(headerRow);
    
    // Data rows
    output.push('├' + '─'.repeat(nameWidth) + '┼' + '─'.repeat(rowsWidth) + '┼' + 
                '─'.repeat(throughputWidth) + '┼' + '─'.repeat(memoryWidth) + '┼' +
                '─'.repeat(typesWidth) + '┼' + '─'.repeat(sampleWidth) + '┤');

    for (const result of results) {
      if (result.error) {
        const errorRow = `│ ${result.name.slice(0, nameWidth - 1).padEnd(nameWidth - 1)}│ ${'---'.padEnd(rowsWidth - 1)}│ ${result.error.slice(0, throughputWidth - 1).padEnd(throughputWidth - 1)}│ ${'---'.padEnd(memoryWidth - 1)}│ ${'---'.padEnd(typesWidth - 1)}│ ${'---'.padEnd(sampleWidth - 1)}│`;
        output.push(errorRow);
        continue;
      }

      // Calculate percentages for bars
      const throughputPct = maxMbPerSec > 0 ? (result.mbPerSec || 0) / maxMbPerSec : 0;
      const memoryPct = maxMemoryDelta > 0 ? ((result.memoryDelta || result.memory?.rss?.peakDelta || 0) / maxMemoryDelta) : 0;

      // Create performance bars
      const throughputBarWidth = 50;
      const memoryBarWidth = 25;
      
      const throughputBar = this.createProgressBar(throughputPct, throughputBarWidth);
      const memoryBar = this.createProgressBar(memoryPct, memoryBarWidth);
      
      // Format values
      const name = result.name.slice(0, nameWidth - 1).padEnd(nameWidth - 1);
      const rowsPerSec = this.formatNumber(result.rowsPerSec || 0).padEnd(rowsWidth - 1);
      const mbPerSecFormatted = this.formatNumber(result.mbPerSec || 0);
      const throughput = `${throughputBar} ${mbPerSecFormatted}`.padEnd(throughputWidth - 1);
      
      const memoryDeltaMiB = ((result.memoryDelta || result.memory?.rss?.peakDelta || 0) / 1024 / 1024);
      const memoryFormatted = this.formatNumber(memoryDeltaMiB, 1);
      const memory = `${memoryBar} ${memoryFormatted}`.padEnd(memoryWidth - 1);
      
      const types = (result.types || ['string']).join(',').slice(0, typesWidth - 1).padEnd(typesWidth - 1);
      
      // Format sample output
      let sample = '---';
      if (result.sample && result.sample.length > 0) {
        try {
          sample = JSON.stringify(result.sample[0]).slice(0, sampleWidth - 1);
        } catch (e) {
          sample = String(result.sample[0]).slice(0, sampleWidth - 1);
        }
      }
      sample = sample.padEnd(sampleWidth - 1);
      
      const row = `│ ${name}│ ${rowsPerSec}│ ${throughput}│ ${memory}│ ${types}│ ${sample}│`;
      output.push(row);
    }

    // Bottom border
    output.push('└' + '─'.repeat(nameWidth) + '┴' + '─'.repeat(rowsWidth) + '┴' + 
                '─'.repeat(throughputWidth) + '┴' + '─'.repeat(memoryWidth) + '┴' +
                '─'.repeat(typesWidth) + '┴' + '─'.repeat(sampleWidth) + '┘');

    return output.join('\n');
  }

  // Convert results to markdown table format
  static formatBenchmarkMarkdown(results, fileInfo) {
    const validResults = results.filter(r => !r.error);
    if (validResults.length === 0) {
      return '**No successful benchmarks to display.**\n\n';
    }

    let output = [];
    
    // Header with file info
    output.push(`### ${fileInfo.file}`);
    output.push(`${this.formatBytes(fileInfo.fileSizeMB * 1024 * 1024, 1)}, ${this.formatNumber(fileInfo.cols || 20, 0)} cols x ${this.formatNumber(fileInfo.rowCount, 0)} rows`);
    output.push('');
    
    // Table header
    output.push('| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |');
    output.push('|--------|----------|--------|--------------|-------|--------|');
    
    for (const result of results) {
      if (result.error) {
        output.push(`| ${result.name} | --- | --- | --- | --- | ERROR: ${result.error} |`);
        continue;
      }
      
      const rowsPerSec = this.formatNumber(result.rowsPerSec || 0);
      const mbPerSec = this.formatNumber(result.mbPerSec || 0, 1);
      const memoryDeltaMiB = ((result.memoryDelta || result.memory?.rss?.peakDelta || 0) / 1024 / 1024);
      const memoryFormatted = this.formatNumber(memoryDeltaMiB, 1);
      const types = (result.types || ['string']).join(',');
      
      let sample = '---';
      if (result.sample && result.sample.length > 0) {
        try {
          sample = JSON.stringify(result.sample[0]).slice(0, 50);
          if (sample.length >= 50) sample += '...';
        } catch (e) {
          sample = String(result.sample[0]).slice(0, 50);
          if (sample.length >= 50) sample += '...';
        }
      }
      
      output.push(`| ${result.name} | ${rowsPerSec} | ${mbPerSec} | ${memoryFormatted} | ${types} | \`${sample}\` |`);
    }
    
    output.push('');
    return output.join('\n');
  }

  // Convert database results to markdown
  static formatDatabaseMarkdown(results, dbType) {
    if (!results || results.length === 0) {
      return `### ${dbType.toUpperCase()} Database Import\n\n**No results available.**\n\n`;
    }

    let output = [];
    output.push(`### ${dbType.toUpperCase()} Database Import`);
    output.push('');
    
    // Table header
    output.push('| Parser | Time | Records/sec | Memory Peak | Status |');
    output.push('|--------|------|-------------|-------------|--------|');
    
    const validResults = results.filter(r => !r.error);
    const fastest = validResults.length > 0 ? 
      validResults.reduce((min, r) => r.duration?.ms < min.duration?.ms ? r : min) : null;

    for (const result of results) {
      if (result.error) {
        output.push(`| ${result.name} | --- | --- | --- | ERROR |`);
        continue;
      }

      const timeRatio = fastest && result !== fastest ? 
        ` (${(result.duration.ms / fastest.duration.ms).toFixed(2)}x)` : '';
      
      const memoryPeak = result.memory?.rss?.formatted?.peak || 
                        result.memory?.heap?.formatted?.peak || 
                        'N/A';

      output.push(`| ${result.name} | ${result.duration.formatted}${timeRatio} | ${result.throughput.recordsPerSecond.toLocaleString()} | ${memoryPeak} | ✓ |`);
    }
    
    output.push('');
    return output.join('\n');
  }

  // Get system information
  static getSystemInfo() {
    const os = require('os');
    const fs = require('fs');
    
    let cpuInfo = 'Unknown';
    let memoryInfo = 'Unknown';
    
    try {
      const cpus = os.cpus();
      if (cpus && cpus.length > 0) {
        cpuInfo = `${cpus[0].model} (${cpus.length} cores)`;
      }
    } catch (e) {
      // Fallback for system info
    }
    
    try {
      const totalMem = os.totalmem();
      memoryInfo = `${Math.round(totalMem / 1024 / 1024 / 1024)}GB`;
    } catch (e) {
      // Fallback for memory info
    }

    return {
      cpu: cpuInfo,
      memory: memoryInfo,
      platform: os.platform(),
      arch: os.arch(),
      nodeVersion: process.version,
      timestamp: new Date().toISOString()
    };
  }

  // Generate complete markdown report
  static generateMarkdownReport(allResults, benchmarkType, size) {
    const systemInfo = this.getSystemInfo();
    let output = [];
    
    // Header
    output.push(`# CSV Parser Benchmark Results`);
    output.push('');
    output.push(`Generated: ${new Date(systemInfo.timestamp).toLocaleString()}`);
    output.push(`Benchmark Type: ${benchmarkType}`);
    output.push(`Dataset Size: ${size}`);
    output.push('');
    
    // System Information
    output.push('## System Information');
    output.push('');
    output.push('| Component | Details |');
    output.push('|-----------|---------|');
    output.push(`| CPU | ${systemInfo.cpu} |`);
    output.push(`| Memory | ${systemInfo.memory} |`);
    output.push(`| OS | ${systemInfo.platform} ${systemInfo.arch} |`);
    output.push(`| Node.js | ${systemInfo.nodeVersion} |`);
    output.push('');
    
    // Benchmark results
    for (const testResult of allResults) {
      if (testResult.isDatabase) {
        // Database benchmark
        const dbType = testResult.name.split(' ')[0].toLowerCase();
        output.push(this.formatDatabaseMarkdown(testResult.results, dbType));
      } else {
        // Parsing benchmark
        const fileInfo = {
          file: testResult.file || 'benchmark',
          fileSizeMB: testResult.fileSizeMB || 0,
          rowCount: testResult.rowCount || 0,
          cols: 20
        };
        output.push(this.formatBenchmarkMarkdown(testResult.results, fileInfo));
      }
    }
    
    // Summary
    if (allResults.length > 0) {
      output.push('## Summary');
      output.push('');
      output.push(this.createSummaryMarkdown(allResults));
    }
    
    return output.join('\n');
  }

  // Create summary in markdown format
  static createSummaryMarkdown(allResults) {
    // Group results by parser
    const parserStats = {};
    
    for (const testResult of allResults) {
      if (!testResult.results) continue;
      
      // Sort results by performance for ranking
      const sortedResults = testResult.results
        .filter(r => !r.error)
        .sort((a, b) => {
          // Sort by fastest time (lower is better)
          const aTime = a.avgTime || a.duration?.ms || Infinity;
          const bTime = b.avgTime || b.duration?.ms || Infinity;
          return aTime - bTime;
        });
      
      for (const result of testResult.results) {
        if (result.error) continue;
        
        if (!parserStats[result.name]) {
          parserStats[result.name] = {
            tests: 0,
            totalRows: 0,
            totalTime: 0,
            avgRowsPerSec: 0,
            avgMbPerSec: 0,
            placements: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, other: 0 },
            weightedScore: 0
          };
        }
        
        const stats = parserStats[result.name];
        stats.tests++;
        stats.totalRows += result.rowsPerSec || 0;
        stats.totalTime += result.avgTime || 0;
        stats.avgRowsPerSec += result.rowsPerSec || 0;
        stats.avgMbPerSec += result.mbPerSec || 0;
        
        // Find placement in this test
        const placement = sortedResults.findIndex(r => r.name === result.name) + 1;
        
        // Point system: 1st=10, 2nd=8, 3rd=6, 4th=4, 5th=2, 6th+=0
        if (placement === 1) {
          stats.placements[1]++;
          stats.weightedScore += 10;
        } else if (placement === 2) {
          stats.placements[2]++;
          stats.weightedScore += 8;
        } else if (placement === 3) {
          stats.placements[3]++;
          stats.weightedScore += 6;
        } else if (placement === 4) {
          stats.placements[4]++;
          stats.weightedScore += 4;
        } else if (placement === 5) {
          stats.placements[5]++;
          stats.weightedScore += 2;
        } else {
          stats.placements.other++;
          stats.weightedScore += 0;
        }
      }
    }
    
    // Calculate averages
    for (const stats of Object.values(parserStats)) {
      if (stats.tests > 0) {
        stats.avgRowsPerSec /= stats.tests;
        stats.avgMbPerSec /= stats.tests;
      }
    }
    
    // Sort by weighted score (higher is better)
    const sortedParsers = Object.entries(parserStats)
      .sort(([,a], [,b]) => b.weightedScore - a.weightedScore);
    
    let output = [];
    output.push('| Parser | Tests | Avg Rows/sec | Avg MB/sec | 1st/2nd/3rd/4th/5th | Score |');
    output.push('|--------|-------|--------------|------------|---------------------|-------|');
    
    for (const [parserName, stats] of sortedParsers) {
      const placementStr = `${stats.placements[1]}/${stats.placements[2]}/${stats.placements[3]}/${stats.placements[4]}/${stats.placements[5]}`;
      output.push(`| ${parserName} | ${stats.tests} | ${this.formatNumber(stats.avgRowsPerSec)} | ${this.formatNumber(stats.avgMbPerSec, 1)} | ${placementStr} | ${stats.weightedScore.toFixed(0)} |`);
    }
    
    output.push('');
    output.push('**Scoring:** 1st=10pts, 2nd=8pts, 3rd=6pts, 4th=4pts, 5th=2pts, 6th+=0pts');
    output.push('');
    
    return output.join('\n');
  }

  // Format database benchmark results (simpler table)
  static formatDatabaseTable(results, testInfo) {
    const Table = require('cli-table3');
    
    const table = new Table({
      head: ['Parser', 'Time', 'Records/sec', 'Memory Peak', 'Status'],
      colWidths: [20, 12, 15, 15, 10]
    });

    const validResults = results.filter(r => !r.error);
    const fastest = validResults.length > 0 ? 
      validResults.reduce((min, r) => r.duration?.ms < min.duration?.ms ? r : min) : null;

    for (const result of results) {
      if (result.error) {
        table.push([result.name, '-', '-', '-', 'ERROR']);
        continue;
      }

      const timeRatio = fastest && result !== fastest ? 
        ` (${(result.duration.ms / fastest.duration.ms).toFixed(2)}x)` : '';
      
      const memoryPeak = result.memory?.rss?.formatted?.peak || 
                        result.memory?.heap?.formatted?.peak || 
                        'N/A';

      table.push([
        result.name,
        result.duration.formatted + timeRatio,
        result.throughput.recordsPerSecond.toLocaleString(),
        memoryPeak,
        '✓'
      ]);
    }

    return table.toString();
  }

  // Create summary report across multiple tests
  static createSummaryReport(allResults) {
    let output = [];
    
    output.push('');
    output.push('═'.repeat(80));
    output.push('BENCHMARK SUMMARY');
    output.push('═'.repeat(80));
    
    // Group results by parser
    const parserStats = {};
    
    for (const testResult of allResults) {
      if (!testResult.results) continue;
      
      // Sort results by performance for ranking
      const sortedResults = testResult.results
        .filter(r => !r.error)
        .sort((a, b) => {
          // Sort by fastest time (lower is better)
          const aTime = a.avgTime || a.duration?.ms || Infinity;
          const bTime = b.avgTime || b.duration?.ms || Infinity;
          return aTime - bTime;
        });
      
      for (const result of testResult.results) {
        if (result.error) continue;
        
        if (!parserStats[result.name]) {
          parserStats[result.name] = {
            tests: 0,
            totalRows: 0,
            totalTime: 0,
            avgRowsPerSec: 0,
            avgMbPerSec: 0,
            placements: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, other: 0 },
            weightedScore: 0
          };
        }
        
        const stats = parserStats[result.name];
        stats.tests++;
        stats.totalRows += result.rowsPerSec || 0;
        stats.totalTime += result.avgTime || 0;
        stats.avgRowsPerSec += result.rowsPerSec || 0;
        stats.avgMbPerSec += result.mbPerSec || 0;
        
        // Find placement in this test
        const placement = sortedResults.findIndex(r => r.name === result.name) + 1;
        
        // Point system: 1st=10, 2nd=8, 3rd=6, 4th=4, 5th=2, 6th+=0
        if (placement === 1) {
          stats.placements[1]++;
          stats.weightedScore += 10;
        } else if (placement === 2) {
          stats.placements[2]++;
          stats.weightedScore += 8;
        } else if (placement === 3) {
          stats.placements[3]++;
          stats.weightedScore += 6;
        } else if (placement === 4) {
          stats.placements[4]++;
          stats.weightedScore += 4;
        } else if (placement === 5) {
          stats.placements[5]++;
          stats.weightedScore += 2;
        } else {
          stats.placements.other++;
          stats.weightedScore += 0; // 6th place and beyond = 0 points
        }
      }
    }
    
    // Calculate averages
    for (const stats of Object.values(parserStats)) {
      if (stats.tests > 0) {
        stats.avgRowsPerSec /= stats.tests;
        stats.avgMbPerSec /= stats.tests;
        stats.avgWeightedScore = stats.weightedScore / stats.tests;
      }
    }
    
    // Sort by weighted score (higher is better)
    const sortedParsers = Object.entries(parserStats)
      .sort(([,a], [,b]) => b.weightedScore - a.weightedScore);
    
    const summaryTable = new (require('cli-table3'))({
      head: ['Parser', 'Tests', 'Avg Rows/sec', 'Avg MB/sec', '1st/2nd/3rd/4th/5th', 'Score'],
      colWidths: [20, 8, 15, 12, 20, 10]
    });
    
    for (const [parserName, stats] of sortedParsers) {
      const placementStr = `${stats.placements[1]}/${stats.placements[2]}/${stats.placements[3]}/${stats.placements[4]}/${stats.placements[5]}`;
      summaryTable.push([
        parserName,
        stats.tests,
        this.formatNumber(stats.avgRowsPerSec),
        this.formatNumber(stats.avgMbPerSec, 1),
        placementStr,
        stats.weightedScore.toFixed(0)
      ]);
    }
    
    output.push(summaryTable.toString());
    output.push('');
    output.push('Scoring: 1st=10pts, 2nd=8pts, 3rd=6pts, 4th=4pts, 5th=2pts, 6th+=0pts');
    output.push('');
    
    return output.join('\n');
  }
}

module.exports = UDSVFormatter;