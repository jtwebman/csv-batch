# CSV Parser Benchmark Results

Generated: 8/2/2025, 4:23:31 PM
Benchmark Type: all
Dataset Size: medium

## System Information

| Component | Details |
|-----------|---------|
| CPU | 12th Gen Intel(R) Core(TM) i9-12900K (4 cores) |
| Memory | 47GB |
| OS | linux x64 |
| Node.js | v22.16.0 |

### litmus_strings.csv
1.9 MiB, 20 cols x 10.0K rows

| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |
|--------|----------|--------|--------------|-------|--------|
| String.split() (arrays) | 1.5M | 288.9 | 0.0 | string | `["TSlZnpHu","9mB","wf4lCxeTjt6xXM","ARgstI","o6qCC...` |
| uDSV (stringObjs) | 1.1M | 209.5 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| d3-dsv | 783.7K | 149.5 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| uDSV (stringArrs) | 698.3K | 133.2 | 0.0 | string | `{"p acCDhKi9AYqIv":"0ZXQhCWJjJEX"," 3DQoXO":"diVdp...` |
| String.split() (objects) | 687.2K | 131.1 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| csv-simple-parser | 585.0K | 111.6 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| papaparse | 436.5K | 83.3 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| but-csv | 430.6K | 82.2 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| comma-separated-values | 351.3K | 67.0 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| csv-batch (sync map) | 256.2K | 48.9 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| csv-batch | 244.9K | 46.7 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| csv-parser | 244.7K | 46.7 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| csv-parse | 163.7K | 31.2 | 0.0 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":" 3DQoXO","wf4...` |
| fast-csv | 102.7K | 19.6 | 20.6 | string | `{"TSlZnpHu":"p acCDhKi9AYqIv","9mB":"3DQoXO","wf4l...` |

### litmus_ints.csv
1 MiB, 20 cols x 10.0K rows

| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |
|--------|----------|--------|--------------|-------|--------|
| String.split() (arrays) | 1.9M | 196.5 | 0.0 | string | `["1qgR6kMRVHrIC","RC783CJha3uAJ","UpAL WZd","M72iq...` |
| uDSV (stringObjs) | 1.7M | 179.7 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| d3-dsv | 1.2M | 125.7 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| csv-simple-parser | 962.1K | 98.9 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| String.split() (objects) | 781.4K | 80.4 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| uDSV (stringArrs) | 732.4K | 75.3 | 0.0 | string | `{"1441":"-6347","1619":"3440","4255":"4472","6292"...` |
| but-csv | 584.6K | 60.1 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| papaparse | 485.8K | 50.0 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| comma-separated-values | 460.0K | 47.3 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| csv-parser | 387.4K | 39.8 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| csv-batch (sync map) | 341.2K | 35.1 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| csv-batch | 311.6K | 32.0 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| csv-parse | 240.5K | 24.7 | 0.0 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |
| fast-csv | 151.5K | 15.6 | 18.8 | string | `{"1qgR6kMRVHrIC":"6292","RC783CJha3uAJ":"-2178","U...` |

### litmus_quoted.csv
1.9 MiB, 20 cols x 10.0K rows

| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |
|--------|----------|--------|--------------|-------|--------|
| String.split() (arrays) | 1.6M | 292.3 | 0.0 | string | `["\"U3Ah5 MIpCGn9\"","\"n9pl T\"","\"hHROC 88d\"",...` |
| uDSV (stringObjs) | 1.1M | 194.2 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| d3-dsv | 616.8K | 114.1 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| uDSV (stringArrs) | 571.8K | 105.8 | 0.0 | string | `{"506":"7208","602":"-3341","3076":"-5273","4455":...` |
| but-csv | 565.3K | 104.6 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| String.split() (objects) | 553.9K | 102.5 | 0.0 | string | `{"\"U3Ah5 MIpCGn9\"":"\"QQUfaa7Mxz\"","\"n9pl T\""...` |
| csv-simple-parser | 475.8K | 88.0 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| comma-separated-values | 317.1K | 58.7 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| csv-batch (sync map) | 300.8K | 55.6 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| csv-batch | 267.4K | 49.5 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| papaparse | 254.7K | 47.1 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| csv-parser | 242.2K | 44.8 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| csv-parse | 158.5K | 29.3 | 0.0 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |
| fast-csv | 106.0K | 19.6 | 22.1 | string | `{"U3Ah5 MIpCGn9":"QQUfaa7Mxz","n9pl T":"-8141","hH...` |

### litmus_mixed.csv
2.2 MiB, 20 cols x 10.0K rows

| Parser | Rows/sec | MB/sec | Memory (MiB) | Types | Sample |
|--------|----------|--------|--------------|-------|--------|
| String.split() (arrays) | 1.5M | 326.5 | 0.0 | string | `["col0","col1","col2","col3","col4","col5","col6",...` |
| String.split() (objects) | 741.3K | 161.3 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| uDSV (stringObjs) | 708.6K | 154.2 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| uDSV (stringArrs) | 594.6K | 129.4 | 0.0 | string | `{"11333":"70482","1dBxciwd0m5CQRe8b1x7":"EosaKyxif...` |
| d3-dsv | 592.9K | 129.0 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| csv-simple-parser | 428.9K | 93.3 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| but-csv | 412.2K | 89.7 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| comma-separated-values | 334.0K | 72.7 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| csv-batch (sync map) | 317.6K | 69.1 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| csv-batch | 309.2K | 67.3 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| papaparse | 306.4K | 66.7 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| csv-parser | 246.3K | 53.6 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| csv-parse | 165.4K | 36.0 | 0.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |
| fast-csv | 106.3K | 23.1 | 19.0 | string | `{"col0":"1dBxciwd0m5CQRe8b1x7","col1":"-92705","co...` |

### SQLITE Database Import

| Parser | Time | Records/sec | Memory Peak | Status |
|--------|------|-------------|-------------|--------|
| csv-batch | 2.27s (1.26x) | 440,733 | 521.71 MB | ✓ |
| csv-parser | 2.31s (1.28x) | 432,482 | 524.55 MB | ✓ |
| fast-csv | 4.27s (2.37x) | 234,168 | 533.50 MB | ✓ |
| papaparse | 1.81s | 553,995 | 536.00 MB | ✓ |
| csv-parse | 3.17s (1.76x) | 315,553 | 536.00 MB | ✓ |

### POSTGRES Database Import

| Parser | Time | Records/sec | Memory Peak | Status |
|--------|------|-------------|-------------|--------|
| csv-batch | 9.79s (54.25x) | 102,118 | 535.07 MB | ✓ |
| csv-parser | 180.50ms | 31,628 | 539.18 MB | ✓ |
| fast-csv | 11.74s (65.05x) | 85,171 | 557.56 MB | ✓ |
| papaparse | 9.11s (50.47x) | 109,763 | 538.17 MB | ✓ |
| csv-parse | 397.42ms (2.20x) | 88,070 | 529.87 MB | ✓ |

## Summary

| Parser | Tests | Avg Rows/sec | Avg MB/sec | 1st/2nd/3rd/4th/5th | Score |
|--------|-------|--------------|------------|---------------------|-------|
| String.split() (arrays) | 4 | 1.6M | 276.1 | 4/0/0/0/0 | 40 |
| uDSV (stringObjs) | 4 | 1.2M | 184.4 | 0/3/1/0/0 | 30 |
| d3-dsv | 4 | 803.9K | 129.6 | 0/0/3/0/1 | 20 |
| papaparse | 6 | 247.2K | 41.2 | 1/0/1/0/0 | 16 |
| csv-parser | 6 | 186.8K | 30.8 | 1/0/1/0/0 | 16 |
| uDSV (stringArrs) | 4 | 649.3K | 110.9 | 0/0/0/3/0 | 12 |
| String.split() (objects) | 4 | 691.0K | 118.8 | 0/1/0/0/2 | 12 |
| csv-batch | 6 | 188.8K | 32.6 | 0/1/0/1/0 | 12 |
| csv-parse | 6 | 121.3K | 20.2 | 0/1/0/1/0 | 12 |
| csv-simple-parser | 4 | 612.9K | 98.0 | 0/0/0/1/0 | 4 |
| fast-csv | 6 | 77.7K | 13.0 | 0/0/0/0/2 | 4 |
| but-csv | 4 | 498.2K | 84.2 | 0/0/0/0/1 | 2 |
| comma-separated-values | 4 | 365.6K | 61.4 | 0/0/0/0/0 | 0 |
| csv-batch (sync map) | 4 | 304.0K | 52.2 | 0/0/0/0/0 | 0 |

**Scoring:** 1st=10pts, 2nd=8pts, 3rd=6pts, 4th=4pts, 5th=2pts, 6th+=0pts
