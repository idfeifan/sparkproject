package spark300.class39;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 二次排序
 * @Author: Liufeifan
 * @Date: 2020/7/23 10:08
 */

public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\IdeaProject\\sparkproject\\src\\main\\java\\spark300\\class39\\sort.txt");

        JavaPairRDD<SecondarySortKey,String> pair = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] lineSplited = line.split(" ");
                System.out.println(lineSplited[0]);
                SecondarySortKey key = new SecondarySortKey(
                        Integer.valueOf(lineSplited[0]) ,
                        Integer.valueOf(lineSplited[1])
                );
                return new Tuple2<SecondarySortKey, String>(key, line) ;
            }
        });

        JavaPairRDD<SecondarySortKey,String> sortedPairs = pair.sortByKey();

        JavaRDD<String> sortedRDD = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        sortedRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
