import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType

object Analytics {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._
    import sqlCtx.implicits._

    // Data preparation
    println("Preparing data")

    val bikeData = sc.textFile("/user/vag273/rbda_proj/bike_grids")
    val bSplit = bikeData.map(line => line.split(','))
    val bCount = bSplit.map(line => (line(1) + '_' + line(2) + '_' + line(8), 1)).reduceByKey(_ + _).map(line => (line(0).split('_')(0), line(0).split('_')(1), line(0).split('_')(2), line(1)))

    val bHeader = Seq("byear", "bmonth", "bzone", "bcount")
    val bDF = bCount.map(line => (line(0), line(1), line(2), line(3))).toDF(bHeader: _*)
    val castBDF = bDF.select(bDF("byear").cast(IntegerType).as("byear"), bDF("bmonth").cast(IntegerType).as("bmonth"), bDF("bzone"), bDF("bcount").cast(LongType).as("bcount"))

    val t2011 = sc.textFile("/user/vag273/rbda_proj/taxi_2011")
    val t2012 = sc.textFile("/user/vag273/rbda_proj/taxi_2012")
    val t2013 = sc.textFile("/user/vag273/rbda_proj/taxi_2013")
    val t2014 = sc.textFile("/user/vag273/rbda_proj/taxi_2014")
    val t2015 = sc.textFile("/user/vag273/rbda_proj/taxi_2015")
    val tData = t2011.union(t2012).union(t2013).union(t2014).union(t2015)
    val tSplit = tData.map(line => line.substring(1, line.length - 1)).map(line => line.split(','))
    val tZone = tSplit.map(line => (line(0) + '-' +  line(1) + '-' +  ((line(3) - 40.69715)/0.003).toInt + "_" + ((line(2) - (-74.022208))/0.003).toInt, 1))
    val tCount = tZone.reduceByKey(_ + _).map(line => (line(0).split('-')(0), line(0).split('-')(1), line(0).split('-')(2), line(1)))

    val tHeader = Seq("tyear", "tmonth", "tzone", "tcount")
    val tDF = tCount.map(line => (line(0), line(1), line(2), line(3))).toDF(tHeader: _*)
    val castTDF = tDF.select(tDF("tyear").cast(IntegerType).as("tyear"), tDF("tmonth").cast(IntegerType).as("tmonth"), tDF("tzone"), tDF("tcount").cast(LongType).as("tcount"))

    val joinDF = castBDF.join(castTDF, castBDF("byear") === castTDF("tyear") && castBDF("bmonth") === castTDF("tmonth"), "full").na.drop(Seq("tyear")).drop(Seq("byear", "bmonth", "bzone")).na.fill(0)

    // Linear Regression
    // var lrRDD = 


    // Linear regression - Overall crime
    println("Overall crime")

    var lrRDD = joinDF.rdd.map(line => (line(9) + "," + line(10) + "," + line(11) + "," + line(12) + "," + line(13), 1)).reduceByKey(_ + _).map(line => line.toString.substring(1, line.toString.length - 1).split(','))

    val lrHeader = Seq("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2", "freq")
    var lrDF = lrRDD.map(line => (line(0), line(0).toInt*line(0).toInt, line(1), line(2), line(3), line(4), line(4).toInt*line(4).toInt, line(5))).toDF(lrHeader: _*)
    var castLrDF = lrDF.select(lrDF("temp").cast(IntegerType).as("temp"), lrDF("temp2").cast(IntegerType).as("temp2"), lrDF("rain").cast(IntegerType).as("rain"), lrDF("snow").cast(IntegerType).as("snow"), lrDF("fog").cast(IntegerType).as("fog"), lrDF("humidity").cast(IntegerType).as("humidity"), lrDF("humidity2").cast(IntegerType).as("humidity2"), lrDF("freq").cast(IntegerType).as("freq"))
  
    val cols = Array("temp", "temp2", "rain", "snow", "fog", "humidity", "humidity2")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    var featuresDF = assembler.transform(castLrDF)

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("freq")

    var lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/project/lr_overall")

    var trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)

    var preds = lrModel.transform(assembler.transform(castFDF)).rdd.map(line => line(0) + "," + line(2) + "," + line(3) + "," + line(4) + "," + line(5) + "," + line(8))
    preds.saveAsTextFile("/user/vag273/project/preds_overall")
  }
}
