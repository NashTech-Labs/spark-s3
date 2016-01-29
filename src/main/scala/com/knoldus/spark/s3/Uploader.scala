package com.knoldus.spark.s3

import java.io.File

import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.transfer.TransferManager

class Uploader(transferManager: TransferManager) extends Serializable {

  def uploadDirectory(file: File, path: String, bucket: String): Boolean = {
    try {
      val transfer = transferManager.uploadDirectory(bucket, path, file, false)
      transfer.waitForCompletion()
      true
    } catch {
      case ex: Exception => false
    }
  }

  def uploadPartition(temporaryFolder: File, path: String, bucket: String): Boolean = {
    try {
      temporaryFolder.listFiles().map { temporaryFiles =>
        temporaryFiles.listFiles().filter(_.getName != "_temporary").map { temporaryFile =>
          temporaryFile.listFiles().map { file =>
            val s3Client = transferManager.getAmazonS3Client
            s3Client.putObject(new PutObjectRequest(bucket, path + "/" + file.getName, file).withCannedAcl(CannedAccessControlList.PublicRead))
          }
        }
      }
      true
    } catch {
      case ex: Exception => false
    }
  }

}
