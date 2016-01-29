package com.knoldus.spark.s3

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.TransferManager
import org.apache.spark.sql.DataFrame

case class UploaderRelation(fileLocation: String, accessKey: String, secretKey: String, bucket: String) {

  def upload(dataFrame: DataFrame): Boolean = {
    val uploader = new Uploader(new TransferManager(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey))))

    uploader.uploadDirectory(new File(fileLocation), fileLocation, bucket)

    dataFrame.foreachPartition { partition =>
      val temporaryFolder = new File(fileLocation + File.separator + "_temporary")
      val partitionUploader = new Uploader(new TransferManager(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey))))
      partitionUploader.uploadPartition(temporaryFolder, fileLocation, bucket)
    }
    true
  }

}
