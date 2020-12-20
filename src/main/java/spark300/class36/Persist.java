package spark300.class36;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Author: Liufeifan
 * @Date: 2020/7/15 17:24
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //cache()或者persist()的使用，是有规则的
        //必须在transformation或者textfile等创建了一个RDDzhihou ,直接连续调用cache()或者persist()才可以
        //如果你先创建一个RDD,然后单独另起一行执行cache()或persist()方法，是没用的，并且会报错。

        //持久化策略  MEMORY_ONLY MEMORY_AND_DISK MEMORY_ONLY_SER

        JavaRDD<String> lines = sc.textFile("C:\\Users\\35807\\Desktop\\spark.txt").cache();
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + "millseconds");


        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + "millseconds");

        sc.close();

    }
}
