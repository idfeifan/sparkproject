package spark300.class35;

import kafka.api.LeaderAndIsr;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Int;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Liufeifan
 * @Date: 2020/7/15 15:44
 */
@SuppressWarnings(value = {"unused","unchecked"})
public class ActionOperation {

    public static void main(String[] args) {
        //reduce();
        map();
    }
    private static void reduce(){
        SparkConf conf = new SparkConf().
                setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        int sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(sum);
        sc.close();
    }
    private static void map(){
        SparkConf conf = new SparkConf()
                .setAppName("map")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numberList);
        JavaRDD<Integer> doubleNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        List<Integer> doubleNumberList = doubleNumberRDD.collect();
        for (int doubleNumber :doubleNumberList){
            System.out.println(doubleNumber);
        }
        sc.close();
    }
}
