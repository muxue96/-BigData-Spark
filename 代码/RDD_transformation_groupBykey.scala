package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_groupBykey {
  def main(args: Array [String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_groupBykey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--groupByKey
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))
    //使用groupByKey之前先把要进行转换的数据集转换成对偶元组的形式(key,value)
    //groupByKey：将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //           元组中的第一个元素就是key
    //           元组中的第二个元素就是相同key的value的集合
    val lastrdd = rdd.groupByKey().map(x => (x._1,x._2.sum))
    lastrdd.collect().foreach(println)
  }
}