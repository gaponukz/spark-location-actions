# Analytics on stream

Stream-table join (stream enrichment) and tumpling window to get something like "most active cities (locations)".

1. Reading stream data from kafka
2. Parsing stream data: log -> table
3. Joining actions stream with user data
4. Windowed Aggregation
5. Output to console in "complete" output mode
