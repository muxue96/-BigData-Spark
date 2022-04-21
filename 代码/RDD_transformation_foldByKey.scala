package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_foldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_foldByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--foldByKey
    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3)
      ,("b",4),("b",5),("a",6)),2)
    //如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    rdd.foldByKey(0)(_+_).collect().foreach(println)
  }
}