package weiboSentiment

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
    /*
    // data1为较小数据， data2为较大数据
    val data1 = sc.parallelize(Array(("1", "aa"),("2", "bb"),("3", "cc")),2)
    val data2 = sc.parallelize(Array(("1", "spark", "hadoop"),("1", "sss", "ssss"),("2", "ElasticSearch", "Flume"),("3", "Kafka", "Redis"),("4", "Flink", "HDFS"),("5", "Yarn", "Linux"),("6", "Windows", "MySQL")),3)
      .map(x=>(x._1, x))
    //data1.join(data2).foreach(println(_))
    val data1Broadcast = sc.broadcast(data1.collectAsMap())
    data2.map(x => {
      (x, data1Broadcast.value.getOrElse(x._1, ""))
    }).filter(_._2!="").foreach(println(_))
     */
    val rdd: RDD[String] = sc.makeRDD(List("w ord","p pp","a aa"))
    val rdd2: RDD[Array[String]] = rdd.map(row => {
      row.split(" ")
    })
    rdd2.collect().foreach(row=>{
      row.foreach(println)
    })
  }

}
