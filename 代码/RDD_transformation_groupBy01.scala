package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_groupBy01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_groupBy01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--groupBy
    //根据单词首字母进行分组
    val rdd = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"),2)
    //分组和分区没有必然的关系
    val groupByRDD = rdd.groupBy(_.charAt(0))
    groupByRDD.collect.foreach(println)

    //groupBy会将数据打乱(打散)，重新组合，这个操作我们称之为shuffle
    sc.stop()
  }
}