import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType

object PrepareDataLinearRegression {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val sqlCtx = new SQLContext(sc)

    import sqlCtx._
    import sqlCtx.implicits._

    // Data preparation
    println("Preparing data")

    val bikeData = sc.textFile("/user/vag273/rbda_proj/bike_clean")
    val bSplit = bikeData.map(line => line.split(','))
    val bCount = bSplit.map(line => (line(1) + '_' + line(2) + '_' + line(8), 1)).reduceByKey(_ + _).map(line => (line._1.split('_')(0), line._1.split('_')(1), line._1.split('_')(2), line._2))

    val bHeader = Seq("byear", "bmonth", "bzone", "bcount")
    val bDF = bCount.map(line => (line._1, line._2, line._3, line._4)).toDF(bHeader: _*)
    val castBDF = bDF.select(bDF("byear").cast(IntegerType).as("byear"), bDF("bmonth").cast(IntegerType).as("bmonth"), bDF("bzone"), bDF("bcount").cast(LongType).as("bcount"))

    val t2013 = sc.textFile("/user/vag273/rbda_proj/taxi_2013")
    val t2014 = sc.textFile("/user/vag273/rbda_proj/taxi_2014")
    val t2015 = sc.textFile("/user/vag273/rbda_proj/taxi_2015")
    val tData = t2013.union(t2014).union(t2015)
    val tSplit = tData.map(line => line.substring(1, line.length - 1)).map(line => line.split(','))
    val tZone = tSplit.map(line => (line(0) + '-' +  line(1) + '-' +  ((line(3).toDouble - 40.69715)/0.003).toInt + "_" + ((line(2).toDouble - (-74.022208))/0.003).toInt, 1))
    val tCount = tZone.reduceByKey(_ + _).map(line => (line._1.split('-')(0), line._1.split('-')(1), line._1.split('-')(2), line._2))

    val tHeader = Seq("tyear", "tmonth", "tzone", "tcount")
    val tDF = tCount.map(line => (line._1, line._2, line._3, line._4)).toDF(tHeader: _*)
    val castTDF = tDF.select(tDF("tyear").cast(IntegerType).as("tyear"), tDF("tmonth").cast(IntegerType).as("tmonth"), tDF("tzone"), tDF("tcount").cast(LongType).as("tcount"))

    val joinDF = castBDF.join(castTDF, castBDF("byear") === castTDF("tyear") && castBDF("bmonth") === castTDF("tmonth") && castBDF("bzone") === castTDF("tzone"), "full").na.drop(Seq("tyear")).drop("byear", "bmonth", "bzone").na.fill(0)
    val finalDF = joinDF.filter(joinDF("bcount") !== 0)

    finalDF.rdd.saveAsTextFile("/user/vag273/rbda_proj/lin_reg_data")
  }
}
