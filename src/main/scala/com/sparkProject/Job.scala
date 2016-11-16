package com.sparkProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Job {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession
      .builder
      //.master("local")
      .appName("spark session TP_parisTech")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._


    /********************************************************************************
      *
      *        TP 1
      *
      *        - Set environment, InteliJ, submit jobs to Spark
      *        - Load local unstructured data
      *        - Word count , Map Reduce
      ********************************************************************************/



    // ----------------- word count ------------------------

    val df_wordCount = sc.textFile("/Users/Wes/spark-2.0.0-bin-hadoop2.6/README.md")
      .flatMap{case (line: String) => line.split(" ")}
      .map{case (word: String) => (word, 1)}
      .reduceByKey{case (i: Int, j: Int) => i + j}
      .toDF("word", "count")

    df_wordCount.orderBy($"count".desc).show()


     /********************************************************************************
      *
      *        TP 2 : début du projet
      *
      ********************************************************************************/


    val df = spark
      .read
      .option("header","true")
      .option("inferSchema","true")
      .option("comment","#")
      .csv("/Users/Wes/CloudStation/big_data/MSBGD/5-INF729_Hadoop/Spark/cumulative.csv")

    val cols = df.columns.slice(10,20)
    //df.select(cols.map(col): _*).show(50)

    //df.printSchema()
    df.groupBy($"koi_disposition").count().show()

    val df_cleaned = df.filter($"koi_disposition" === "CONFIRMED" || $"koi_disposition" === "FALSE POSITIVE")
    val cols_cleaned = df_cleaned.columns.slice(0,10)
    df_cleaned.select(cols_cleaned.map(col): _*).show(50)

    df.groupBy($"koi_eccen_err1").count().show()

    val df_cleaned2 = df_cleaned.drop($"koi_eccen_err1")

    val df_cleaned3 = df_cleaned2.drop("index","kepid","koi_fpflag_nt","koi_fpflag_ss","koi_fpflag_co","koi_fpflag_ec","koi_sparprov",
      "koi_trans_mod","koi_datalink_dvr","koi_datalink_dvs","koi_tce_delivname","koi_parm_prov","koi_limbdark_mod",
      "koi_fittype","koi_disp_prov","koi_comment","kepoi_name","kepler_name","koi_vet_date","koi_pdisposition")

    df_cleaned3.printSchema()

    val useless_column = df_cleaned3.columns.filter{ case(column:String) =>
      df_cleaned3.agg(countDistinct(column)).first().getLong(0) <= 1}

    val df_cleaned4 = df_cleaned3.drop(useless_column: _*)

    print("df4" + df_cleaned4.columns.length)

    df_cleaned4.describe("koi_impact","koi_duration").show()

    val df_filled = df_cleaned4.na.fill(0.0)

    val df_labels = df_cleaned4.select("rowid","koi_disposition")
    val df_features = df_cleaned4.drop("koi_disposition")
    val df_joined = df_features.join(df_labels, usingColumn = "rowid")

    def udf_sum = udf((col1: Double, col2: Double) => col1 + col2)

    val df_newFeatures = df_joined.withColumn("koi_ror_min", udf_sum($"koi_ror",$"koi_ror_err2"))
      .withColumn("koi_ror_max",$"koi_ror" + $"koi_ror_err1")

    df_newFeatures.coalesce(1)
      .write
      .mode("overwrite")
      .option("header","true")
      .csv("/Users/Wes/CloudStation/big_data/MSBGD/5-INF729_Hadoop/Spark/tp_spark/tp2_spark.csv")

  }


}
