package com.knoldus.spark.s3

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.specs2.mock.Mockito

class S3RelationTest extends FunSuite with Mockito with BeforeAndAfterEach {

  var sparkConf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override def beforeEach() {
    sparkConf = new SparkConf().setMaster("local").setAppName("Test S3 Relation")
    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
  }

  override def afterEach() {
    sc.stop()
  }


  test("get schema") {
    val dataFrame = sqlContext.read.json(getClass.getResource("/sample.json").getPath)
    val relation = S3Relation("path", "access_key", "secretKey", "bucket", "json", dataFrame, sqlContext)
    val result = relation.schema
    assert(result === dataFrame.schema)
  }

}
