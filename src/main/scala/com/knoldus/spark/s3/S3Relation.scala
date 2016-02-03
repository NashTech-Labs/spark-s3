package com.knoldus.spark.s3

import org.apache.log4j.Logger
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

case class S3Relation(path: String,
                      accessKey: String,
                      secretKey: String,
                      bucket: String,
                      fileType: String,
                      dataFrame: DataFrame,
                      sqlContext: SQLContext) extends BaseRelation {

  private val logger = Logger.getLogger(classOf[S3Relation])

  override def schema: StructType = dataFrame.schema
}
