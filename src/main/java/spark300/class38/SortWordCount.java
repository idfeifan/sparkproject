package spark300.class38;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Liufeifan
 * @Date: 2020/7/16 15:53
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textRDD = sc.textFile("C:\\Users\\35807\\Desktop\\spark.txt");
        JavaRDD<String> wordRDD = textRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 8888821824952235508L;

            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String ,Integer> wordPairRDD = wordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairRDD<String,Integer> wordCountRDD = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer,String> countWordRDD =wordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<Integer, String>(tuple2._2,tuple2._1);
            }
        });
        JavaPairRDD<Integer,String> sortCountWordRDD = countWordRDD.sortByKey(false);
        JavaPairRDD<String,Integer> sortWordCountRDD =sortCountWordRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(Tuple2<Integer, String> tup2) throws Exception {
                return new Tuple2<String,Integer>(tup2._2,tup2._1);
            }
        });
        List<Tuple2<String,Integer>> listsortWordCountRDD = sortWordCountRDD.take(10);
        for (Tuple2<String,Integer> tup :listsortWordCountRDD){
            System.out.println(tup);
        }
    }
}
