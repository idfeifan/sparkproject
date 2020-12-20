package com.ibeifeng;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.util.ArrayList;

/**
 * @Author: Liufeifan
 * @Date: 2020/12/20 13:47
 */
public class Spark2Test {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                                .builder()
                                .appName("Spark2Test")
                                .master("local")
                                .enableHiveSupport()
                                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

     //   SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
     //   JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> studentsRDD = jsc.textFile("C:\\Users\\35807\\Desktop\\students.txt");

        JavaRDD<Row> RDDRow = studentsRDD.map(line ->
                {
                    String[] a = line.split(",");
                    System.out.println(a[0]);
                    System.out.println(a[1]);
                    return RowFactory.create(Integer.valueOf(a[0]),String.valueOf(a[1]),Integer.valueOf(a[2]));
                });

        ArrayList<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(fields);

        Dataset studentsDF  = spark.createDataFrame(RDDRow,structType);

        studentsDF.createOrReplaceTempView("students");

        Dataset<Row> result = spark.sql("select * from students");

        result.show();


        //System.out.println(result.collect());



    }
}
