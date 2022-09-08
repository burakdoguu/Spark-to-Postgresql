# Spark-to-Postgresql
Spark stream write to Postgresql on Local Computer

Firstly, Some modules to be used in the process should be imported.
As a default in streaming process schemaInference will be off, it can be added as a configuration but more effiecent way is create a schema by yourself. 
Structured Streaming process has several options, one of them is "maxFilesPerTrigger" as known maximum number of new files to be considered in every trigger. If you have multiple files to read, I recommend using this feature. After the reading process, The columns that we will transfer to the database are separated. 
All data has been successfully transferred to the postgresql database. The result of some null values means that there is no data in the json part, not from the process.
