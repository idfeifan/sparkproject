package spark300.DataFrame.class77;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * @Author: Liufeifan
 * @Date: 2020/12/17 21:17
 */
public class ParquetLoadfData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetLoadData");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame usersDF = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users.parquet");
        usersDF.registerTempTable("users");
        DataFrame userNamesDF = sqlContext.sql("select name from users");
        List<String> userNames = userNamesDF.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row x) throws Exception {
                return x.getString(0);
            }
        }).collect();

        for (String username :userNames){
            System.out.println(username);
        }
    }
}
