# CSV Parser Benchmark Results

Generated: 1/2/2025, 3:45:23 PM
Benchmark Type: all
Dataset Size: medium

## System Information

| Component | Details |
|-----------|---------|
| CPU | Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz (8 cores) |
| Memory | 32GB |
| OS | linux x64 |
| Node.js | v18.19.0 |

### litmus_strings.csv
1.6 MiB, 20 cols x 10.0K rows

| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |
|--------|----------|--------|--------------|-------|--------|
| csv-batch | 890.2K | 142.4 | 8.5 | string | `{"col0":"value_0_0","col1":"value_0_1","col2":"value_0_2"...` |
| uDSV | 1.2M | 192.8 | 12.1 | string | `{"col0":"value_0_0","col1":"value_0_1","col2":"value_0_2"...` |
| csv-parser | 654.1K | 104.7 | 15.2 | string | `{"col0":"value_0_0","col1":"value_0_1","col2":"value_0_2"...` |
| fast-csv | 723.8K | 115.8 | 11.8 | string | `{"col0":"value_0_0","col1":"value_0_1","col2":"value_0_2"...` |

### litmus_ints.csv
1.2 MiB, 20 cols x 10.0K rows

| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |
|--------|----------|--------|--------------|-------|--------|
| csv-batch | 1.1M | 132.5 | 7.8 | string | `{"col0":"123","col1":"456","col2":"789","col3":"012"...` |
| uDSV | 1.4M | 168.9 | 10.5 | number | `{"col0":123,"col1":456,"col2":789,"col3":12,"col4":345...` |
| csv-parser | 743.2K | 89.5 | 14.1 | string | `{"col0":"123","col1":"456","col2":"789","col3":"012"...` |
| fast-csv | 856.7K | 103.2 | 10.9 | string | `{"col0":"123","col1":"456","col2":"789","col3":"012"...` |

### SQLITE Database Import

| Parser | Time | Records/sec | Memory Peak | Status |
|--------|------|-------------|-------------|--------|
| csv-batch | 1.23s | 812,195 | 45.2 MiB | ✓ |
| csv-parser | 1.89s (1.54x) | 529,101 | 68.7 MiB | ✓ |
| fast-csv | 1.67s (1.36x) | 599,401 | 52.3 MiB | ✓ |
| papaparse | 2.15s (1.75x) | 465,116 | 71.2 MiB | ✓ |
| csv-parse | 1.78s (1.45x) | 561,798 | 49.8 MiB | ✓ |

### POSTGRESQL Database Import

| Parser | Time | Records/sec | Memory Peak | Status |
|--------|------|-------------|-------------|--------|
| csv-batch | 2.45s | 408,163 | 52.1 MiB | ✓ |
| csv-parser | 3.12s (1.27x) | 320,513 | 76.4 MiB | ✓ |
| fast-csv | 2.89s (1.18x) | 346,021 | 58.9 MiB | ✓ |
| papaparse | 3.67s (1.50x) | 272,480 | 82.3 MiB | ✓ |
| csv-parse | 3.01s (1.23x) | 332,226 | 61.2 MiB | ✓ |

## Summary

| Parser | Tests | Avg Rows/sec | Avg MB/sec | 1st/2nd/3rd/4th/5th | Score |
|--------|-------|--------------|------------|---------------------|-------|
| csv-batch | 4 | 777.6K | 145.2 | 2/1/1/0/0 | 34 |
| uDSV | 2 | 1.3M | 180.9 | 2/0/0/0/0 | 20 |
| fast-csv | 4 | 631.5K | 120.8 | 0/1/2/1/0 | 22 |
| csv-parser | 4 | 561.7K | 104.9 | 0/0/1/2/1 | 12 |
| papaparse | 2 | 368.8K | 85.6 | 0/0/0/1/1 | 6 |
| csv-parse | 2 | 447.0K | 98.2 | 0/2/0/0/1 | 18 |

**Scoring:** 1st=10pts, 2nd=8pts, 3rd=6pts, 4th=4pts, 5th=2pts, 6th+=0pts