package spark300;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Liufeifan
 * @Date: 2020/3/23 20:42
 */
public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JSONDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //针对json文件创建DataFrame
        DataFrame studentScoresDF = sqlContext.read().json("hdfs://h201:9000/spark_study/student.json");

        //针对学生成绩信息的DataFrame,注册临时表，查询成绩大于80的学生的姓名
        studentScoresDF.registerTempTable("student_scores");
        DataFrame goodStudentNamesDF = sqlContext.sql("select name,score from student_scores where score>= 80");
        List<String> goodStudentNamesList = goodStudentNamesDF.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.getString(0);
            }
        }).collect();
        //然后针对JavaRDD<String> 创建DataFrame
        //（针对包含json串的JavaRDD，创建DataFrame）
        List<String> studentInfoJSONs  = new ArrayList<String>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");
        JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs);
        DataFrame studentInfosDF =sqlContext.read().json(studentInfoJSONsRDD);

        //针对学生基本信息DataFrame，注册临时表，然后查询分数大于80分的学生的基本信息
        studentInfosDF.registerTempTable("student_infos");

        String sql = "select name ,age from student_infos where name in (";
        for (int i =0 ; i < goodStudentNamesList.size(); i++){
            sql += "'" + goodStudentNamesList.get(i) + "'";
            if (i < goodStudentNamesList.size()-1) {
                sql += ",";
            }
        }
        sql +=")";
        DataFrame goodStudentInfoDF = sqlContext.sql(sql);
        // 然后将两份数据的DataFrame，转换为JavaPairRDD，执行join transformation
        // （将DataFrame转换为JavaRDD，再map为JavaPairRDD，然后进行join）
        JavaPairRDD<String,Tuple2<Integer,Integer>> goodStudentsRDD = goodStudentNamesDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                @Override
                public scala.Tuple2<String, Integer> call(Row row) throws Exception {
                    return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
                }
            }).join(goodStudentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                @Override
                public scala.Tuple2<String, Integer> call(Row row) throws Exception {
                    return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
                }
            }));
         // 然后将封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
         // （将JavaRDD，转换为DataFrame）
        JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1,v1._2._1,v1._2._2);
            }
        });

        //创建一份元数据，将JavaRDD<Row>转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD,structType);
        // 将好学生的全部信息保存到一个json文件中去
        // （将DataFrame中的数据保存到外部的json文件中去）
        goodStudentsDF.write().format("json").save("hdfs://192.168.1.5:9000/spark_study/good-students");
    }
}
