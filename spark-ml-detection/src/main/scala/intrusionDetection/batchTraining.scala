package intrusionDetection

import common._
import intrusionDetection.featurePreprocessing._
import intrusionDetection.utility._
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.NANOSECONDS

object batchTraining {
  def main(args: Array[String]): Unit = {

    if(!args(0).startsWith("hdfs") && !args(0).startsWith("s3")){ //Khong check input neu input den tu hdfs hoac s3
      parseBatchArgs(args)
    }

    val spark = SparkSession.builder()
      .appName("Model Training")
      //.master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")

    val trainFile = args(0)
    val modelsLocation = args(1)

    /*******************************
    ** Training file preparation ***
    ********************************/

    val trainDF = spark.read
      .schema(kddDataSchema)
      .csv(trainFile)

    //Drop bo cac raw hoac cac raw co gia tri null or nan 
    println("Preparing training file")
    val cleanTrainDF = trainDF
      .drop(col("score"))
      .na.drop("any")

    //Thay label bang ten nhom tan cong
    val replacedLabelsTrainDF = cleanTrainDF.withColumn("label", categorizeKdd2Labels(col("label")))
      .cache()

    //tao cac chi muc cho categorical kdd features
    val stringIndexer = indexCategoricalKdd(replacedLabelsTrainDF)

    //Tạo Discretizer cho các tính năng kdd lớn liên tục cho Chi Square Selection
    val discretizer = discretizeLargeContinuousKdd(replacedLabelsTrainDF)

    //Hợp cac tinh nang cho mo hinh (khong roi rac)
    //va cho chi square selection (co roi rac)
    val stages = Array(stringIndexer, discretizer)
    val pipelineModel = assembleKdd(replacedLabelsTrainDF, stages)
    pipelineModel.write.overwrite().save(modelsLocation + "/pipelineModel")

    println("Feature preprocessing")
    val assembledTrainDF = pipelineModel.transform(replacedLabelsTrainDF)

    //Chi Squared feature selection
    println("Applying Chi-squared selection")
    val chiSqModel = applyChiSqSelection2(assembledTrainDF,10)
    chiSqModel.write.overwrite().save(modelsLocation + "/chiSqModel")

    //chọn features voi chiSq model
    val selectedTrainDF = chiSqModel.transform(assembledTrainDF)

    /********************************
     ******** Model training ********
     ********************************/

    //Decision tree training
    println("Training Decision Tree...")
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label_Indexed")
      .setFeaturesCol("selectedFeatures")
      //.setWeightCol("classWeight")
      .setMaxBins(71)
      .setMaxDepth(10)
    val dt_time = System.nanoTime
    val dtModel = dt.fit(selectedTrainDF)
    println(s"Decision Tree trained in: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime - dt_time)} secs\n")
    dtModel.write.overwrite().save(modelsLocation + "/dtModel")

    //Random forest training
    println("Training Random Forest...")
    val rf = new RandomForestClassifier()
      .setLabelCol("label_Indexed")
      .setFeaturesCol("selectedFeatures")
      .setMaxBins(71)
      .setMaxDepth(7)
      //.setWeightCol("classWeight")
      //.setNumTrees(30)
    val rf_time = System.nanoTime
    val rfModel = rf.fit(selectedTrainDF)
    println(s"Random Forest trained in: ${TimeUnit.NANOSECONDS.toSeconds(System.nanoTime - rf_time)} secs\n")
    rfModel.write.overwrite().save(modelsLocation + "/rfModel")

    println(s"Models saved in ${modelsLocation}")
    spark.stop()
  }

}
