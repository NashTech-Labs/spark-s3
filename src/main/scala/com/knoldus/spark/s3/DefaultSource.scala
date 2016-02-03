/*
 * Copyright 2016 Knoldus
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.knoldus.spark.s3

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.TransferManager
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider{

  @transient val logger = Logger.getLogger(classOf[DefaultSource])

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    sys.error("Read is not yet supported")
    new BaseRelation {
      override def sqlContext: SQLContext = sqlContext
      override def schema: StructType = schema
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): S3Relation = {
    val accessKey = parameters.getOrElse("accessKey", sys.error("Amazon S3 Access Key has to be provided"))
    val secretKey = parameters.getOrElse("secretKey", sys.error("Amazon S3 Secret Key has to be provided"))
    val bucket = parameters.getOrElse("bucket", sys.error("Amazon S3 Bucket has to be provided"))
    val fileType = parameters.getOrElse("fileType", sys.error("File Type has to be provided"))
    val path = parameters.getOrElse("path", sys.error(s"'path' must be specified for $fileType data."))

    val supportedFileTypes = List("json", "parquet")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val hadoopFileSystemPath = new Path(path)
    val fileSystem = hadoopFileSystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val save =
      if (fileSystem.exists(hadoopFileSystemPath)) {
        mode match {
          case SaveMode.Append => sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
          case SaveMode.Overwrite => fileSystem.delete(hadoopFileSystemPath, true);true
          case SaveMode.ErrorIfExists => sys.error(s"path $path already exists.")
          case SaveMode.Ignore => false
        }
      } else {
        true
      }

    if(save) {
      write(fileType, data, hadoopFileSystemPath.toString)

      uploadToS3(accessKey, secretKey, bucket, hadoopFileSystemPath.toString, data)
    }

    S3Relation(path, accessKey, secretKey, bucket, fileType, data, sqlContext)
  }

  private def write(fileType: String, dataFrame: DataFrame, path: String) = {
    if (fileType.equals("json")) {
      dataFrame.write.json(path)
    } else if (fileType.equals("parquet")) {
      dataFrame.write.parquet(path)
    }
  }

  private def uploadToS3(accessKey: String, secretKey: String, bucket: String, path: String, dataFrame: DataFrame) = {
    val uploader = new Uploader(new TransferManager(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey))))

    uploader.uploadDirectory(new File(path), path, bucket)

    dataFrame.foreachPartition { partition =>
      val temporaryFolder = new File(path + File.separator + "_temporary")
      val partitionUploader = new Uploader(new TransferManager(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey))))
      partitionUploader.uploadPartition(temporaryFolder, path, bucket)
    }
  }
}
