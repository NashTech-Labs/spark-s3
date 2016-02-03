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
            s3Client.putObject(new PutObjectRequest(bucket, path + File.separator + file.getName, file).withCannedAcl(CannedAccessControlList.PublicRead))
          }
        }
      }
      true
    } catch {
      case ex: Exception => false
    }
  }

}
