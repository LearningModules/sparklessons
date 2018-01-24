/**
  * Created by reji on 24/01/18.
  */
import org.apache.spark._;
import org.apache.log4j._
object WordCount{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","WordCount")
    val input = sc.textFile("input/input.txt")
    val wordRDD=input.flatMap( x=> x.split(" "))
    val wordPairRDD = wordRDD.map( x => (x,1) )
    val wordCount = wordPairRDD.reduceByKey(_+_).map(x=>(x._2,x._1.toString.replace(".",""))).sortByKey(ascending = false)
    wordCount.collect().foreach(println)
    println("Total Words : "+wordCount.count())
  }
}
