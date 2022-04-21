package SparkCore.RDD转换算子

import org.apache.spark.{SparkConf, SparkContext}

import java.util.Date
import java.text.SimpleDateFormat

object RDD_transformation_groupBy02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD_transformation_groupBy02").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //TODO 算子--groupBy
    val rdd = sc.textFile("file:///export/servers/spark/temp/sgg/apache.log")
    val timeRDD = rdd.map(
        line => {
          val datas = line.split(" ")
          val time = datas(3)
          val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val date = sdf.parse(time)//parse将字符串转换为时间
          val sdf1 = new SimpleDateFormat("HH")
          val hour = sdf1.format(date)//format将时间转换为字符串
          (hour,1)
        }
      ).groupBy(_._1)
    timeRDD.map{
      case (hour,iter) => {
        (hour,iter.size)
      }
    }.collect().foreach(println)
    sc.stop()
  }
}