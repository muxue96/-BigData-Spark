package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_flatMap02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_flatMap02").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--flatMap
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val flatRDD = rdd.flatMap(
      data => {
        data match{
          case list:List[_] => list
          case dat => List(dat)
        }
      }
    )
    flatRDD.collect().foreach(println)

    sc.stop()
  }
}