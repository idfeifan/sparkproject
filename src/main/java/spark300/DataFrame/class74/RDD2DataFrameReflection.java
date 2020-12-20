package spark300.DataFrame.class74;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * 使用反射的方式将RDD转化为DataFrame
 * @Author: Liufeifan
 * @Date: 2020/9/10 11:27
 */
public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        //创建普通的RDD
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameReflection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\35807\\Desktop\\students.txt");
        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            private static final long serialVersionUID = -4380072655662960755L;

            @Override
            public Student call(String line) throws Exception {
                String [] words = line.split(",");
                Student stu = new Student();
                stu.setAge(Integer.valueOf(words[2]));
                stu.setId(Integer.valueOf(words[0]));
                stu.setName(String.valueOf(words[1]));
                return stu;
            }
        });
        //将RDD 转化为DataFrame
        DataFrame df = sqlContext.createDataFrame(students,Student.class);

        //注册临时表
        df.registerTempTable("students");
        //使用sql查询
        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        //DataFrame 转化为RDD
        JavaRDD<Row> teenagerRDD = teenagerDF.toJavaRDD();
        //将Row类型RDD转化为反射类型
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            private static final long serialVersionUID = 2506150525993089005L;

            @Override
            public Student call(Row v1) throws Exception {
                Student stu  = new Student();
                stu.setAge(v1.getInt(0));
                stu.setId(v1.getInt(1));
                stu.setName(v1.getString(2));
                return stu;
            }
        });
        //打印
        List<Student> list = teenagerStudentRDD.collect();
        for (Student ss: list) {
            System.out.println(ss);
        }
    }




}
