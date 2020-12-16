import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

object Analytics {
  def encode(line: String): Array[Long] = {
    var lSplit = line.split(',')
    var out = Array(lSplit(0).toLong, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, lSplit(2).toLong)
    out(lSplit(1).toInt) = 1

    return out
  }

  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._
    import sqlCtx.implicits._

    // Data preparation
    println("Preparing data")

    val bikeData = sc.textFile("/user/vag273/rbda_proj/bike_grids")
    val bSplit = bikeData.map(line => line.split(','))
    val bCount = bSplit.map(line => (line(1) + '_' + line(2) + '_' + line(8), 1)).reduceByKey(_ + _).map(line => (line._1.split('_')(0), line._1.split('_')(1), line._1.split('_')(2), line._2))

    val bHeader = Seq("byear", "bmonth", "bzone", "bcount")
    val bDF = bCount.map(line => (line._1, line._2, line._3, line._4)).toDF(bHeader: _*)
    val castBDF = bDF.select(bDF("byear").cast(IntegerType).as("byear"), bDF("bmonth").cast(IntegerType).as("bmonth"), bDF("bzone"), bDF("bcount").cast(LongType).as("bcount"))

    val t2011 = sc.textFile("/user/vag273/rbda_proj/taxi_2011")
    val t2012 = sc.textFile("/user/vag273/rbda_proj/taxi_2012")
    val t2013 = sc.textFile("/user/vag273/rbda_proj/taxi_2013")
    val t2014 = sc.textFile("/user/vag273/rbda_proj/taxi_2014")
    val t2015 = sc.textFile("/user/vag273/rbda_proj/taxi_2015")
    val tData = t2011.union(t2012).union(t2013).union(t2014).union(t2015)
    val tSplit = tData.map(line => line.substring(1, line.length - 1)).map(line => line.split(','))
    val tZone = tSplit.map(line => (line(0) + '-' +  line(1) + '-' +  ((line(3).toDouble - 40.69715)/0.003).toInt + "_" + ((line(2).toDouble - (-74.022208))/0.003).toInt, 1))
    val tCount = tZone.reduceByKey(_ + _).map(line => (line._1.split('-')(0), line._1.split('-')(1), line._1.split('-')(2), line._2))

    val tHeader = Seq("tyear", "tmonth", "tzone", "tcount")
    val tDF = tCount.map(line => (line._1, line._2, line._3, line._4)).toDF(tHeader: _*)
    val castTDF = tDF.select(tDF("tyear").cast(IntegerType).as("tyear"), tDF("tmonth").cast(IntegerType).as("tmonth"), tDF("tzone"), tDF("tcount").cast(LongType).as("tcount"))

    val joinDF = castBDF.join(castTDF, castBDF("byear") === castTDF("tyear") && castBDF("bmonth") === castTDF("tmonth") && castBDF("bzone") === castTDF("tzone"), "full").na.drop(Seq("tyear")).drop("byear", "bmonth", "bzone").na.fill(0)

    // Linear Regression
    var lrRDD = joinDF.rdd.map(line => encode(line(0) + "," + line(2) + "," + line(4)))

    val lrHeader = Seq("bcount", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "ijan", "ifeb", "imar", "iapr", "imay", "ijun", "ijul", "iaug", "isep", "ioct", "inov", "idec", "tcount")
    var lrDF = lrRDD.map(line => Seq(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(1)*line(0), line(2)*line(0), line(3)*line(0), line(4)*line(0), line(5)*line(0), line(6)*line(0), line(7)*line(0), line(8)*line(0), line(9)*line(0), line(10)*line(0), line(11)*line(0), line(12)*line(0), line(13))).toDF(lrHeader: _*)
    
    val cols = Array("bcount", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "ijan", "ifeb", "imar", "iapr", "imay", "ijun", "ijul", "iaug", "isep", "ioct", "inov", "idec")
    val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
    var featuresDF = assembler.transform(lrDF)

    val lr = new LinearRegression().setFeaturesCol("features").setLabelCol("tcount")

    var lrModel = lr.fit(featuresDF)
    lrModel.write.overwrite().save("/user/vag273/rbda_proj/lr_overall")

    var trainSummary = lrModel.summary
    println("R2: " + trainSummary.r2)
  }
}
