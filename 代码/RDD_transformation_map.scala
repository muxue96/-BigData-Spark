package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_map").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO算子 -map
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //1)第一种方式，先声明一个函数，再把函数传进去
    def mapFunction(num: Int): Int = {
      num * 2
    }

    val mapRDD1 = rdd.map(mapFunction)
    mapRDD1.collect().foreach(println)
    //2)第二种方式，使用匿名函数
    val mapRDD2 = rdd.map((num: Int) => {
      num * 2
    })
    /*    scala中的至简原则：1.方法体只有一行的时候，可以去掉花括号，val mapRDD2 = rdd.map((num:Int)=> num*2)
                      2.如果参数类型可以推断出来，类型可以省略，val mapRDD2 = rdd.map((num)=> num*2)
                      3.如果参数列表只有一个参数，那么可以省略括号，val mapRDD2 = rdd.map(num=> num*2)
                      4.如果参数在逻辑中只出现一次而且是按照顺序出现的，参数可以用下划线代替val mapRDD2 = rdd.map(_*2)*/
    mapRDD2.collect().foreach(println)
    sc.stop()
  }
}