package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

object RDD_transformation_sortBy01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_sortBy01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--sortBy
    val rdd = sc.makeRDD(List(("1",1),("11",2),("2",3)),2)
    val sorttRDD = rdd.sortBy(t => t._1,false)
    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式，true升序，改为false(降序)
    //sortBy默认情况下，不会改变分区。但是中间存在shuffle操作

    //根据两个参数排序
    //先按照长度降序排，长度相等时使用字母表的升序排
    val dataRDD  = List("hello","world","atguigu","aaa","bbb")
    val dataRDD1 = dataRDD.sortBy(x =>(x.length,x))(Ordering.Tuple2(Ordering.Int.reverse,Ordering.String))
    //注意:在使用sortBy时，如果用第二个括号也要加参数的时候，前面的数据不能是RDD类型
    //如果前面是RDD类型，那我们就用collect采集结果成array类型
    //如：violation2.collect.sortBy(x=>(x._2._1,x._2._2))(Ordering.Tuple2(Ordering.Int.reverse,Ordering.Int))
    sorttRDD.collect().foreach(println)
    sc.stop()
  }
}