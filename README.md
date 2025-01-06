# Analytics on stream

Stream-table join (stream enrichment) and tumpling window to get something like "most active cities (locations)".

1. Reading stream data from kafka
2. Parsing stream data: log -> table
3. Joining actions stream with user data
4. Splitting streams by boolean condition
5. Windowed Aggregations
6. Output to console in "complete" output mode

## Future Problem to Solve: Up-to-Date Users Table
<b>Problem Statement:</b> The actions stream is real-time, but the users table is not. As a result, it is possible that after some time, we may encounter actions with `user_id` values that no longer exist in the local users table.

<b> Bad Solution:</b> Enrich the data for every event by making a request to the database for the user data. Such remote queries are likely to be slow and can lead to database overload.

<b> Good Solution:</b> Maintain a local copy of the user data for stream processing, enabling local queries without network round-trip latency. Instead of loading the table as a DataFrame in Spark, consider building a hash index that is updated by another stream, ensuring real-time consistency of the user data.

<b> Problem I Faced:</b>  How to store an in-memory or local index efficiently? How can I simultaneously update it and read values quickly? The core question, however, is: <b>How do I use it from Spark?</b>
