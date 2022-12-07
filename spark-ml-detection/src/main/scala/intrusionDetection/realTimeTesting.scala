package intrusionDetection

import common.{kddDataSchema, outputCols}
import intrusionDetection.utility._
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel, VectorSlicer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, from_csv}
import org.apache.spark.sql.streaming.Trigger
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object realTimeTesting {
  def main(args: Array[String]): Unit = {

    parseStreamArgs(args)
    val es_host = args(4)

    val spark = SparkSession.builder()
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "changeme")
      .config(ConfigurationOptions.ES_NODES, es_host)
      .config(ConfigurationOptions.ES_PORT, "9200")
      .config(ConfigurationOptions.ES_NODES_WAN_ONLY, true)
      .appName("Real Time Testing")
      //.master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")

    val pipelineLocation = args(1) + "/pipelineModel"
    val chiSqLocation = args(1) + "/chiSqModel"
    val dtLocation = args(1) + "/dtModel"
    val rfLocation = args(1) + "/rfModel"


    val pipelineModel = PipelineModel.load(pipelineLocation)
    val chiSqModel = VectorSlicer.load(chiSqLocation)
    val dtModel = DecisionTreeClassificationModel.load(dtLocation)
    val rfModel = RandomForestClassificationModel.load(rfLocation)

    val topicName = args(0)
    val mlAlgorithm = args(2)
    val kafkaHost = args(3)

    /*******************************
     *    Chuan bi luong dau vao   *
     *******************************/

    val kafkaTestDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)//.option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topicName)//.option("subscribe", "netflows")
      .option("startingOffsets", "earliest")
      .load()

    //Lay ten cua cot
    val options = Map("delimiter" -> ",")
    //Chia cac cot, phan tach theo dau ',', có Schema la luoc do da duoc dinh nghia ben file common, va chi dinh cac value se la kieu text,string.
    val schemaTestDF = kafkaTestDF
      .select(from_csv(col("value").cast("String"),kddDataSchema,options) as "csv")
      .select("csv.*")

    //Drop hang score va hang co gia tri null
    val cleanTestDF = schemaTestDF
      .drop(col("score"))
      .na.drop("any")

    //Thay the tat ca cac nhan tan cong voi cac the loai khac nhau thanh "attack"
    val replacedLabelsTestDF = cleanTestDF
      .withColumn("label", categorizeKdd2Labels(col("label")))

    //Tinh nang tien xu ly cac luong den voi mo hinh duong ong duoc xay dung.
    val assembledTestDF = pipelineModel.transform(replacedLabelsTestDF)

    //Chon cac tinh nang tot nhat voi mo hinh chi-squared duoc xay dung
    val selectedTestDF = chiSqModel.transform(assembledTestDF)

    /******************************
     *** Ap dung mo hinh du doan ***
     ******************************/

    val predictions = mlAlgorithm match {
      case "dt" => dtModel.transform(selectedTestDF)
      case _ => rfModel.transform(selectedTestDF)
    }

    //Giai doan convert du doan so thanh chu
    val indexer = pipelineModel.stages(0).asInstanceOf[StringIndexerModel]
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(indexer.labelsArray(3))
    val convertedPredictions = labelConverter.transform(predictions)


    //them timestamp va output
    println("Now Streaming ...")
    if (es_host == "console"){
      convertedPredictions.withColumn("timestamp", current_timestamp())
        .writeStream
        .outputMode("append")
        .format("console")
        .start()
        .awaitTermination()
      // convertedPredictions.select(col("predictedLabel"))
      //   .writeStream
      //   .outputMode("append")
      //   .format("console")
      //   .start()
      //   .awaitTermination()
    } else {
      convertedPredictions.withColumn("timestamp", current_timestamp())
        .select(outputCols.map(c => col(c)): _*)
        .writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("checkpointLocation", "/tmp/")
        .start("kdddata")
        .awaitTermination()
      // convertedPredictions.select(col("predictedLabel"))
      //   .select(outputCols.map(c => col(c)): _*)
      //   .writeStream
      //   .outputMode("append")
      //   .format("org.elasticsearch.spark.sql")
      //   .option("checkpointLocation", "/tmp/")
      //   .start("kdddata1")
      //   .awaitTermination()
    }

  }
}