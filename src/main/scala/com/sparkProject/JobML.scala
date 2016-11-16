package com.sparkProject
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import scala.math._

/**
  * Created by Wes on 02/11/2016.
  */
object JobML {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark2 = SparkSession
      .builder
      //.master("local")
      .appName("spark session TP_parisTech")
      .getOrCreate()

    val sc2 = spark2.sparkContext


     /********************************************************************************
      *
      *        TP 3/4 : suite du projet
      *
      ********************************************************************************/


    //Import des données
    val df = spark2
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("comment", "#")
      .csv(args(0))
      //.csv("/Users/Wes/CloudStation/big_data/MSBGD/5-INF729_Hadoop/Spark/tp_spark/tp2_spark.csv/part-r-00000-c9479a35-d21c-4763-8712-ee8e27e6616a.csv")

    // Suppression des champs null
    val df_cleaned_null = df.na.fill(0.0)

    //a. Mise en forme des colonnes

    //scop >

    //List(df_cleaned_null.columns.filter(column=("koi_disposition", "rowid"))).toArray.foreach(println)
    //println(df_cleaned_null.drop("koi_disposition", "rowid").columns)

    val name_features = df_cleaned_null.columns.filter(_!= "koi_disposition").filter(_!="rowid")

    /*
    val vecAssembler = new VectorAssembler()
            .setInputCols(Array("koi_period",	"koi_period_err1",	"koi_period_err2",	"koi_time0bk",
            "koi_time0bk_err1",	"koi_time0bk_err2",	"koi_time0",	"koi_time0_err1",	"koi_time0_err2",	"koi_impact",
            "koi_impact_err1",	"koi_impact_err2",	"koi_duration",	"koi_duration_err1",	"koi_duration_err2",
            "koi_depth",	"koi_depth_err1",	"koi_depth_err2",	"koi_ror",	"koi_ror_err1",	"koi_ror_err2",	"koi_srho",
            "koi_srho_err1",	"koi_srho_err2",	"koi_prad",	"koi_prad_err1",	"koi_prad_err2",	"koi_sma",	"koi_incl",
            "koi_teq",	"koi_insol",	"koi_insol_err1",	"koi_insol_err2",	"koi_dor",	"koi_dor_err1",	"koi_dor_err2",
            "koi_ldm_coeff2",	"koi_ldm_coeff1",	"koi_max_sngle_ev",	"koi_max_mult_ev",	"koi_model_snr",	"koi_count",
            "koi_num_transits",	"koi_tce_plnt_num",	"koi_quarters",	"koi_steff",	"koi_steff_err1",	"koi_steff_err2",
            "koi_slogg",	"koi_slogg_err1",	"koi_slogg_err2",	"koi_smet",	"koi_smet_err1",	"koi_smet_err2",
            "koi_srad",	"koi_srad_err1",	"koi_srad_err2",	"koi_smass",	"koi_smass_err1",	"koi_smass_err2",	"ra",
            "dec",	"koi_kepmag",	"koi_gmag",	"koi_rmag",	"koi_imag",	"koi_zmag",	"koi_jmag",	"koi_hmag",	"koi_kmag",
            "koi_fwm_stat_sig",	"koi_fwm_sra",	"koi_fwm_sra_err",	"koi_fwm_sdec",	"koi_fwm_sdec_err",	"koi_fwm_srao",
            "koi_fwm_srao_err",	"koi_fwm_sdeco",	"koi_fwm_sdeco_err",	"koi_fwm_prao",	"koi_fwm_prao_err",
            "koi_fwm_pdeco",	"koi_fwm_pdeco_err",	"koi_dicco_mra",	"koi_dicco_mra_err",	"koi_dicco_mdec",
            "koi_dicco_mdec_err",	"koi_dicco_msky",	"koi_dicco_msky_err",	"koi_dicco_fra",	"koi_dicco_fra_err",
            "koi_dicco_fdec",	"koi_dicco_fdec_err",	"koi_dicco_fsky",	"koi_dicco_fsky_err",	"koi_dikco_mra",
            "koi_dikco_mra_err",	"koi_dikco_mdec",	"koi_dikco_mdec_err",	"koi_dikco_msky",	"koi_dikco_msky_err",
            "koi_dikco_fra",	"koi_dikco_fra_err",	"koi_dikco_fdec",	"koi_dikco_fdec_err",	"koi_dikco_fsky",
            "koi_ror_min",	"koi_ror_max"))
            .setOutputCol("features")
*/

    val vecAssembler = new VectorAssembler()
      .setInputCols(name_features)
      .setOutputCol("features")

    val df_vector = vecAssembler.transform(df_cleaned_null)
    //println(df_vector.select("features", "koi_disposition").first())

    //df_vector.printSchema()

    // b. Centrer­réduire
    // > Probleme de sparsité - contournement :
    // Cette opération est réalisée par l'option setStandardization du modele de régression logistique

    //val scaler = new StandardScaler()
    //  .setInputCol("features")
    //  .setOutputCol("scaledFeatures")
    //  .setWithStd(true)
    //  .setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    //val scalerModel = scaler.fit(df_vector)

    // Normalize each feature to have unit standard deviation.
    //val scaledData = scalerModel.transform(df_vector)
    //println()
    //println(scaledData.first())

    //scaledData.printSchema()


    // c. Travailler avec les chaînes de caractères

    //create the indexed DataFrame
    val indexer = new StringIndexer().setInputCol("koi_disposition").setOutputCol("label").fit(df_vector)
    val indexed = indexer.transform(df_vector)


    // 2. Machine Learning

    // a. Splitter les données en Training Set et Test Set

    // Split the data into training and test sets (10% held out for testing)
    val Array(trainingData, testData) = indexed.randomSplit(Array(0.9, 0.1))

    //trainingData.groupBy("koi_disposition").count().show()
    //testData.groupBy("koi_disposition").count().show()

    // b. Entraînement du classifieur et réglage des hyper­paramètres de l’algorithme.

    val lr = new LogisticRegression()
      .setElasticNetParam(1.0)  // L1-norm regularization : LASSO
      .setLabelCol("label")
      .setStandardization(true)  // to scale each feature of the model
      .setFitIntercept(true)  // we want an affine regression
      .setTol(1.0e-5)  // stop criterion of the algorithm based on its convergence
      .setMaxIter(300)  // a security stop criterion to avoid infinite loops

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // this grid will have 12 parameters settings for CrossValidator to choose from.
    val val_scale = (-6.0 to 0.0 by 0.5).toArray
    val val_scale_final = val_scale.map({x:Double => math.pow(10, x)})
    //val_scale_final.foreach(println)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, val_scale_final)
      .build()

    val metricVal = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction")

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(metricVal)
      .setEstimatorParamMaps(paramGrid)
      // 70% of the data will be used for training and the remaining 30% for validation.
      .setTrainRatio(0.7)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(trainingData)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    val df_WithPredictions = model.transform(testData)
      .select("features", "label", "prediction")

    df_WithPredictions.show()
    df_WithPredictions.groupBy("label", "prediction").count.show()
    println("\nModel Accuracy : " + metricVal.evaluate(df_WithPredictions).toString())

  }
}
