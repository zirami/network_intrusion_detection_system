package intrusionDetection

import common._
import intrusionDetection.utility.asDense
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{Bucketizer, ChiSqSelector, ChiSqSelectorModel, OneHotEncoder, QuantileDiscretizer, StringIndexer, StringIndexerModel, VectorAssembler, VectorSlicer}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.linalg.DenseVector
import org.apache.commons.math3.distribution.ChiSquaredDistribution

object featurePreprocessing {

  def indexCategoricalKdd(df: DataFrame): StringIndexerModel = {

    //Phân loại các features -> Numerical
    val categoricalIndexedCols = categoricalCols.map(_ + "_Indexed")

    val indexer = new StringIndexer()
      .setInputCols(categoricalCols)
      .setOutputCols(categoricalIndexedCols)
      .fit(df)

    indexer
  }

  def discretizeLargeContinuousKdd(df: DataFrame): Bucketizer = {

    val discretizedLargeContinuousCols = largeContinuousCols.map(_ + "_Discretized")

    val discretizer =  new QuantileDiscretizer()
      .setInputCols(largeContinuousCols)
      .setOutputCols(discretizedLargeContinuousCols)
      .setNumBuckets(10000)
      .fit(df)

    discretizer
  }

  def assembleKdd(df: DataFrame, stages: Array[_ <: PipelineStage]): PipelineModel = {

    val assemblerForSelection = new VectorAssembler()
      .setInputCols(assembledForSelectionCols)
      .setOutputCol("featuresForSelection")

    val assembler = new VectorAssembler()
      .setInputCols(assembledCols)
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(stages :+ assemblerForSelection :+ assembler)

    val pipelineModel = pipeline.fit(df)
    pipelineModel
  }

  def applyChiSqSelection1(df: DataFrame): ChiSqSelectorModel = {

    val chiSqTest = ChiSquareTest.test(df, "featuresForSelection", "label_Indexed").head
    val pValues = chiSqTest.getAs[DenseVector](0)

    //Chúng ta bắt đầu với pValue thấp nhất làm ngưỡng và tăng nó cho đến khi ta nhận được ít nhất một nửa số tính năng
    var pValueThreshold = pValues.values.min
    val half_of_Total_Features = (pValues.size * 0.5).toInt

    //Chọn ít nhất một nửa tổng số tính năng để giảm thiểu việc loại bỏ các tính năng quan trọng có thể có
    while (pValues.values.count(_ <= pValueThreshold) < half_of_Total_Features) {
      pValueThreshold += 0.001
    }

    val selector = new ChiSqSelector()
      .setSelectorType("fpr")
      .setFpr(pValueThreshold + 0.000001) //Thêm một số nhỏ để bao gồm threshold trong các giá trị đã chọn
      .setFeaturesCol("featuresForSelection")
      .setLabelCol("label_Indexed")
      .setOutputCol("selectedFeatures")

    val chiSqModel = selector.fit(df)
    val importantFeatures = chiSqModel.selectedFeatures
    println(s"Selected ${importantFeatures.size} out of ${pValues.size} total features which are: \n " +
      s"${importantFeatures.mkString("(", ", ", ")")}")

    chiSqModel
  }

  def applyChiSqSelection2(df: DataFrame, numFeatures: Int): VectorSlicer  = {

    val chiSqTest = ChiSquareTest.test(df, "featuresForSelection", "label_Indexed").head
    val pValues = chiSqTest.getAs[DenseVector](0)
    val degreesOfFreedom = chiSqTest.getSeq[Int](1)//.mkString(", ")
    val chiSqStats = chiSqTest.getAs[DenseVector](2)

//    println(s"pValues = $pValues \n")
//    println(s"Degrees of Freedom = $degreesOfFreedom \n")
//    println(s"Chi squared stats = $chiSqStats \n")

    val mappedStats = chiSqStats.values.zipWithIndex
    val sortedStats = mappedStats.sortBy(_._1).reverse

    val selectedFeatures = sortedStats.map(_._2).take(numFeatures).sorted
    println(s"Selected features ${selectedFeatures.mkString(",")}")

    val chiSqModel = new VectorSlicer()
      .setIndices(selectedFeatures)
      .setInputCol("features")
      .setOutputCol("selectedFeatures")

    chiSqModel
    }

}
