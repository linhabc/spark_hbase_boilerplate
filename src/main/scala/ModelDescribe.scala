import org.apache.spark.ml.tuning.CrossValidatorModel

object ModelDescribe {
  def main(args: Array[String]): Unit = {
    // load random forest model
    val cvModelLoaded = CrossValidatorModel
      .load("/user/MobiScore_Output/post_payment/post_payment_model")
    println(cvModelLoaded.bestModel.toString())
  }
}
