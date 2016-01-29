package com.knoldus.spark.s3

import org.apache.spark.sql.DataFrame

case class UploaderRelation(fileLocation: String, fileType: String) {

  def upload(dataFrame: DataFrame): Boolean = {
    fileType match {
      case "json" =>
        dataFrame.write.json(fileLocation)
        true
      case "parquet" =>
        dataFrame.write.parquet(fileLocation)
        true
      case _ => false
    }
  }

}
