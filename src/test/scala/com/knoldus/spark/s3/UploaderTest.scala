package com.knoldus.spark.s3

import java.io.File

import com.amazonaws.AmazonClientException
import com.amazonaws.services.s3.model.{PutObjectRequest, PutObjectResult}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.transfer.{MultipleFileUpload, TransferManager}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.specs2.mock.Mockito

class UploaderTest extends FunSuite with Mockito {

  private val transferManager = mock[TransferManager]
  private val amazonS3Client = mock[AmazonS3Client]
  private val multipleFileUpload = mock[MultipleFileUpload]
  private val putObjectResult = mock[PutObjectResult]
  private val file = new File("src/test/resources/sample.csv")
  private val filePath = "src/test/resources/sample.csv"
  private val bucket = "bucket"
  private val uploader = new Uploader(transferManager)

  test("upload is successful") {
    when(transferManager.uploadDirectory(any[String], any[String], any[File], any[Boolean])).thenReturn(multipleFileUpload)
    val result = uploader.upload(file, filePath, bucket)
    assert(result)
  }

  test("upload is not successful") {
    when(transferManager.uploadDirectory(any[String], any[String], any[File], any[Boolean])).thenThrow(new IllegalArgumentException)
    val result = uploader.upload(file, filePath, bucket)
    assert(!result)
  }

  test("upload partition is successful") {
    when(transferManager.getAmazonS3Client).thenReturn(amazonS3Client)
    when(amazonS3Client.putObject(any[PutObjectRequest])).thenReturn(putObjectResult)
    val result = uploader.uploadPartition(new File("src"), filePath, bucket)
    assert(result)
  }

  test("upload partition is not successful") {
    when(transferManager.getAmazonS3Client).thenReturn(amazonS3Client)
    when(amazonS3Client.putObject(any[PutObjectRequest])).thenThrow(new AmazonClientException("exception"))
    val result = uploader.uploadPartition(new File("src"), filePath, bucket)
    assert(!result)
  }

}
