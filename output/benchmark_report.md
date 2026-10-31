# Benchmark report

Dataset: 20,000 orders -> 53,911 valid events + 4,313 labelled bad/duplicate rows, 37 daily batch files (seed 42). Final target size: 20,000 rows. All three targets reconciled: **PASS**.

Each batch is ingested once; then each strategy publishes it. Only the publish step is timed.

| strategy | total publish (s) | median per batch (ms) | mean per batch (ms) | last (tiny) batch (ms) | rows read | rows written | rows deleted |
|----------|------------------:|----------------------:|--------------------:|-----------------------:|----------:|-------------:|-------------:|
| full_refresh | 5.46 | 135 | 147 | 288.6 | 1,148,692 | 447,428 | 0 |
| incremental_merge | 0.71 | 13 | 19 | 0.4 | 53,911 | 43,504 | 0 |
| partition_replace | 2.84 | 89 | 77 | 12.5 | 367,187 | 157,500 | 137,500 |

Ingestion of all batches: 3.54 s. Generation: 0.53 s.

## Optimization: composite index on raw_events(order_date, seq)

Reading 5 partitions out of the landed history: 24.5 ms with the index vs 66.7 ms without (**2.7x**). Partition replacement and the late-data path depend on it.

## Per-batch publish time (ms)

| batch | events | full_refresh | incremental_merge | partition_replace | partitions rewritten (partition_replace) |
|-------|-------:|-------------:|------------------:|------------------:|-----------------------------------------:|
| orders_2026-01-01.csv | 771 | 4 | 6 | 4 | 1 |
| orders_2026-01-02.csv | 1229 | 8 | 9 | 12 | 2 |
| orders_2026-01-03.csv | 1598 | 12 | 13 | 24 | 3 |
| orders_2026-01-04.csv | 1821 | 22 | 12 | 30 | 4 |
| orders_2026-01-05.csv | 1894 | 29 | 14 | 55 | 5 |
| orders_2026-01-06.csv | 2033 | 62 | 13 | 49 | 6 |
| orders_2026-01-07.csv | 1986 | 74 | 16 | 62 | 7 |
| orders_2026-01-08.csv | 1974 | 55 | 18 | 91 | 8 |
| orders_2026-01-09.csv | 1905 | 66 | 12 | 93 | 8 |
| orders_2026-01-10.csv | 1943 | 94 | 13 | 69 | 8 |
| orders_2026-01-11.csv | 1918 | 73 | 12 | 96 | 8 |
| orders_2026-01-12.csv | 1951 | 106 | 16 | 70 | 8 |
| orders_2026-01-13.csv | 1908 | 88 | 12 | 82 | 7 |
| orders_2026-01-14.csv | 1944 | 124 | 14 | 69 | 8 |
| orders_2026-01-15.csv | 1919 | 102 | 13 | 92 | 8 |
| orders_2026-01-16.csv | 1958 | 110 | 16 | 98 | 8 |
| orders_2026-01-17.csv | 1929 | 118 | 14 | 118 | 9 |
| orders_2026-01-18.csv | 1909 | 124 | 13 | 108 | 9 |
| orders_2026-01-19.csv | 1936 | 135 | 13 | 101 | 8 |
| orders_2026-01-20.csv | 1998 | 140 | 14 | 105 | 7 |
| orders_2026-01-21.csv | 1918 | 154 | 53 | 89 | 8 |
| orders_2026-01-22.csv | 2001 | 170 | 53 | 84 | 8 |
| orders_2026-01-23.csv | 2014 | 166 | 77 | 128 | 9 |
| orders_2026-01-24.csv | 1880 | 228 | 17 | 98 | 8 |
| orders_2026-01-25.csv | 1813 | 167 | 60 | 102 | 9 |
| orders_2026-01-26.csv | 1945 | 185 | 51 | 102 | 8 |
| orders_2026-01-27.csv | 1954 | 239 | 57 | 104 | 8 |
| orders_2026-01-28.csv | 1977 | 194 | 18 | 114 | 8 |
| orders_2026-01-29.csv | 1867 | 247 | 16 | 123 | 8 |
| orders_2026-01-30.csv | 1925 | 244 | 15 | 106 | 8 |
| orders_2026-01-31.csv | 1174 | 250 | 10 | 102 | 7 |
| orders_2026-02-01.csv | 744 | 250 | 8 | 54 | 5 |
| orders_2026-02-02.csv | 319 | 309 | 4 | 88 | 5 |
| orders_2026-02-03.csv | 109 | 257 | 3 | 45 | 4 |
| orders_2026-02-04.csv | 49 | 275 | 2 | 35 | 3 |
| orders_2026-02-05.csv | 8 | 285 | 0 | 23 | 2 |
| orders_2026-02-06.csv | 3 | 289 | 0 | 12 | 1 |
