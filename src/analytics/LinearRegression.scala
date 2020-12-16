import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

object LinearRegression {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._
    import sqlCtx.implicits._

    // Loading data
    val data = sc.textFile("/user/vag273/rbda_proj/lin_reg_data")
    val baseDF = data.map(line => line.substring(1, line.length - 1).split(',')).map(line => (line(0).toLong, line(2).toInt, line(4).toLong)).toDF("bcount", "tmonth", "tcount")

    // Linear Regression - Jan
    println("Regression - January")

    var lrDF = baseDF.filter(baseDF("tmonth") === 1).drop("tmonth")

    val cols = Array("bcount")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    var featuresDF = assembler.transform(lrDF)

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("tcount")

    var lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_jan")

    var trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Feb
    println("Regression - February")

    lrDF = baseDF.filter(baseDF("tmonth") === 2).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_feb")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Mar
    println("Regression - March")

    lrDF = baseDF.filter(baseDF("tmonth") === 3).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_mar")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Apr
    println("Regression - April")

    lrDF = baseDF.filter(baseDF("tmonth") === 4).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_apr")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - May
    println("Regression - May")

    lrDF = baseDF.filter(baseDF("tmonth") === 5).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_may")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Jun
    println("Regression - June")

    lrDF = baseDF.filter(baseDF("tmonth") === 6).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_jun")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Jul 
    println("Regression - July")

    lrDF = baseDF.filter(baseDF("tmonth") === 7).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_jul")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Aug
    println("Regression - August")

    lrDF = baseDF.filter(baseDF("tmonth") === 8).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_aug")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Sep
    println("Regression - September")

    lrDF = baseDF.filter(baseDF("tmonth") === 9).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_sep")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Oct
    println("Regression - October")

    lrRDF = baseDF.filter(baseDF("tmonth") === 10).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_oct")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Nov
    println("Regression - November")

    lrRDF = baseDF.filter(baseDF("tmonth") === 11).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_nov")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    // Linear Regression - Dec
    println("Regression - December")

    lrRDF = baseDF.filter(baseDF("tmonth") === 12).drop("tmonth")
    
    featuresDF = assembler.transform(lrDF)

    lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_dec")

    trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)
  }
}
