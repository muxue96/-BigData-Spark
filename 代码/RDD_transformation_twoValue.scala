package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_twoValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_twoValue").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--双值类型算子，intersection，union，subtract，zip
    //交集，并集和差集要求两个数据源类型保持一致，拉链没有要求
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))

    //交集 4,3
    val intersectionRDD = rdd1.intersection(rdd2)
    print(intersectionRDD.collect().mkString(","))
    //并集1,2,3,4,3,4,5,6
    val unionRDD = rdd1.union(rdd2)
    print(unionRDD.collect().mkString(","))
    //差集1,2   5,6
    val subtractRDD1 = rdd1.subtract(rdd2)
    print(subtractRDD1.collect().mkString(","))
    val subtractRDD2 = rdd2.subtract(rdd1)
    print(subtractRDD2.collect().mkString(","))
    //拉链(1,3),(2,4),(3,5),(4,6)
    //两个数据源要求分区数量要保持一致，两个数据源要求分区中数据数量保持一致
    val zipRDD = rdd1.zip(rdd2)
    print(zipRDD.collect().mkString(","))
    sc.stop()
  }
}