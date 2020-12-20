package class35

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: Liufeifan
  * @Date: 2020/7/15 15:54
  */
object ActionOperation {
  def main(args: Array[String]): Unit = {
    reduce()
  }
  def reduce(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numberArray =Array(1,2,3,4,5,6,7,8,9,10)
    val numberRDD = sc.parallelize(numberArray,3)
    val sum = numberRDD.reduce(_ + _)
    print(sum)
  }
}
