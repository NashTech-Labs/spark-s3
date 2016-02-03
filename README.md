# Spark S3 Connector Library

A library for uploading dataframes to Amazon S3.

## Requirements

This library requires Spark 1.4+

## Features
This package can be used to upload dataframe to Amazon S3
This library requires following options:
* `accessKey`: Amazon S3 Access Key. 
* `secretKey`: Amazon S3 Secret Key.
* `bucket`: Amazon S3 Secret Bucket name.
* `fileType`: Type of the file. Supported types are json and parquet

### Scala API
Spark 1.4+:
```scala
dataFrame.write
    .format("com.knoldus.spark.s3")
    .option("accessKey","amazon_s3_access_key")
    .option("secretKey","amazon_s3_secret_key")
    .option("bucket","amazon_s3_bucket")
    .option("fileType","json")
    .save("/tmp/test.json")
```
