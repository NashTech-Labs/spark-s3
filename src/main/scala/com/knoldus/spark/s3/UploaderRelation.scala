package com.knoldus.spark.s3

import org.apache.spark.sql.{DataFrame, SQLContext}

case class UploaderRelation(fileLocation: String, fileType: String, sqlContext: SQLContext) {

  def upload(dataFrame: DataFrame) =
    fileType match {
      case "json" => dataFrame.write.json(fileLocation)
      case "parquet" => dataFrame.write.parquet(fileLocation)
    }

}
