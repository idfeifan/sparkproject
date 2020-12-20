package spark300.DataFrame.class79;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 合并元数据
 * @Author: Liufeifan
 * @Date: 2020/12/17 21:52
 */
public class ParquetMergeSchema {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ParquetPartitionDiscovery");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(jsc);

        //手动创建一个DataFrame
        List<String> studentsWithNameAge = Arrays.asList("(\"leo\",23)","(\"jack\",35)");
        JavaRDD<String> studentsWithNameAgeRDD = jsc.parallelize(studentsWithNameAge);
        JavaRDD<Row> studentsWithNameAgeRDD2 = studentsWithNameAgeRDD.map(new Function<String, Row>() {
            private static final long serialVersionUID = 5832625588363845325L;

            @Override
            public Row call(String v1) throws Exception {
                String[] line = v1.split(",");
                String name = line[0];
                Integer age  = Integer.valueOf(line[1]);
                return RowFactory.create(name,age);
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(fields);

        DataFrame studentsWithNameAgeRDD2DF = sqlContext.createDataFrame(studentsWithNameAgeRDD2,structType);

        studentsWithNameAgeRDD2DF.save("hdfs://h201:9000/spark-study/students","parquet", SaveMode.Append);



        //手动创建第二个DataFrame
        List<String> studentsWithNameGrade = Arrays.asList("(\"leo\",\"A\")","(\"jack\",\"B\")");
        JavaRDD<String> studentsWithNameGradeRDD = jsc.parallelize(studentsWithNameGrade);
        JavaRDD<Row> studentsWithNameGradeRDD2 = studentsWithNameGradeRDD.map(new Function<String, Row>() {
            private static final long serialVersionUID = -6949168856516785835L;

            @Override
            public Row call(String v1) throws Exception {
                String[] line = v1.split(",");
                String name = line[0];
                String grade  = String.valueOf(line[1]);
                return RowFactory.create(name,grade);
            }
        });

        fields.clear();

        //ArrayList<StructField> fields2 = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("grade",DataTypes.StringType,true));
        StructType structType2 = DataTypes.createStructType(fields);

        DataFrame studentsWithNameGradeRDD2DF = sqlContext.createDataFrame(studentsWithNameGradeRDD2,structType2);

        studentsWithNameGradeRDD2DF.save("hdfs://h201:9000/spark-study/students","parquet", SaveMode.Append);

        DataFrame studentsDF = sqlContext.read().option("mergeSchema","true").parquet("hdfs://h201:9000/spark-study/students");
        studentsDF.printSchema();
        studentsDF.show();

    }
}
