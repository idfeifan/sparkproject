package spark300.DataFrame.class81;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive数据源
 * @Author: Liufeifan
 * @Date: 2020/12/20 15:14
 */
public class HiveDataSource {
    public static void main(String[] args) {
        //首先还是创建sparkconf
        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");

        //创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建HiveContext
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop if exists table student_infos");

        sc.close();
    }
}
