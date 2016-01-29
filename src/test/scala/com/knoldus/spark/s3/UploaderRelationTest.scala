package com.knoldus.spark.s3

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.specs2.mock.Mockito

class UploaderRelationTest extends FunSuite with Mockito with BeforeAndAfterAll {

  private val relation = UploaderRelation("path", "json")
  private val sparkContext = new SparkContext(new SparkConf().setAppName("spark-s3-test").setMaster("local"))
  private val sqlContext = new SQLContext(sparkContext)
  private val dataFrame = sqlContext.read.json("src/test/resources/sample.json")
  private val file = new File("path")

  override def beforeAll: Unit = {
    if(file.exists() && file.isDirectory) {
      Option(file.listFiles()).map(_.toList).getOrElse(Nil).foreach(_.delete())
      file.delete()
    }
  }

  test("upload is successful") {
    val result = relation.upload(dataFrame)
    assert(result)
  }

  override def afterAll: Unit = {
    if(file.exists() && file.isDirectory) {
      Option(file.listFiles()).map(_.toList).getOrElse(Nil).foreach(_.delete())
      file.delete()
    }
  }

}
