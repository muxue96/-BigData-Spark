package SparkCore.RDD转换算子

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_groupBy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--groupBy
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    //相同的可以、值的数据会放置在一个组中
    //根据奇偶数分组
    val groupByRDD = rdd.groupBy(x => x%2)
    groupByRDD.collect().foreach(println)
    sc.stop()
  }
}