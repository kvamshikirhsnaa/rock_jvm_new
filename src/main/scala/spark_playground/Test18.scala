package spark_playground

import org.apache.spark.sql.SparkSession

object Test18 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      master( "local" ).
      appName( "sample" ).getOrCreate()

    spark.sparkContext.setLogLevel( "ERROR" )

    import spark.implicits._

    val df = spark.range(100)
    df.show
  }

}
