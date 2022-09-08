# Spark-to-Postgresql
Spark stream write to Postgresql on Local Computer

Firstly, Some modules to be used in the process should be imported.
As a default in streaming process schemaInference will be off, it can be added as a configuration but more effiecent way is create a schema by yourself. 
Structured Streaming process has several options, one of them is "maxFilesPerTrigger" as known maximum number of new files to be considered in every trigger. If you have multiple files to read, I recommend using this feature. After the reading process, The columns that we will transfer to the database are separated. 
All data has been successfully transferred to the postgresql database. The result of some null values means that there is no data in the json part, not from the process.

![1](https://user-images.githubusercontent.com/73526595/189175534-0002d4e6-a378-4669-94d9-6820e2781f77.jpeg)
![schema-2](https://user-images.githubusercontent.com/73526595/189175557-762503a6-1a94-4136-8e3d-b7f8c7368ce0.jpeg)
![yaazılandatalar](https://user-images.githubusercontent.com/73526595/189175569-2bf0a863-ad85-4ccc-983e-be078cb15936.jpeg)
