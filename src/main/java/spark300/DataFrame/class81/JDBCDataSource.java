package spark300.DataFrame.class81;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author: Liufeifan
 * @Date: 2020/12/20 15:28
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        Map<String ,String> options = new HashMap<String, String>();
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/spark");
        options.put("user","root");
        options.put("password","123456");
        options.put("dbtable","task");


        DataFrame studentScoreDF = sqlContext.read().options(options).format("jdbc").load();
        studentScoreDF.show();




    }
}
