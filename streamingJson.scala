package BasicTasks.sparkStreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingJson {
  var count=0
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("streamingJson")
    val ssc = new StreamingContext(conf, Seconds(10))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("ec2-35-154-96-163.ap-south-1.compute.amazonaws.com", 2222)
    //name age city
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      if (x.isEmpty()) {
        val t = spark.read.json(x).toDF()
        t.printSchema()
        t.createOrReplaceTempView("tab")
        val res = spark.sql("select * from tab")
        res.show()
        res.coalesce(1).write.mode(SaveMode.Append).option("delimiter", "|").csv("/tmp/jsonTab")
      }else{
        println("No Data recieved this time")
      }
      count+=1
      val finalPath="C:\\tmp\\jsonPer"+count+".csv"
      println(finalPath)
      println(count)
      if(count>=3){
        count=0
        println("time to merge")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}