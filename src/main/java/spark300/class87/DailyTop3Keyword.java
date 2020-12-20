package spark300.class87;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.util.*;

/**
 * @Author: Liufeifan
 * @Date: 2020/7/24 14:42
 */
public class DailyTop3Keyword {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DailyTop3Keyword");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hct = new HiveContext(sc.sc());

        //伪造出一份数据，查询条件
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform",Arrays.asList("android"));
        queryParamMap.put("version",Arrays.asList("1.0","1.2","1.5","2.0"));

        //将查询参数Map封装成Broadcast,广播变量，每个节点一分即可，不需要每个task一个
        final Broadcast<Map<String,List<String>>> queryParamMapBroadcast =
                sc.broadcast(queryParamMap);

        //针对Hdfs文件中的日志，获取输入RDD
        JavaRDD<String> rawRDD = sc.textFile("hdfs://h201:9000/spark-study/keyword.txt");

        //使用查询参数Map广播变量，进行筛选
        JavaRDD<String> filterRDD = rawRDD.filter(new Function<String, Boolean>() {
            private static final long serialVersionUID = -2441389132608471233L;

            @Override
            public Boolean call(String log) throws Exception{
                //切割原始日志，获取城市/平台 和版本
                String[] logSplited = log.split("\t");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];

                Map<String,List<String>> queryParamMap = queryParamMapBroadcast.value();
                List<String> cities = queryParamMap.get("city");
                if (cities.size() > 0 && !cities.contains(city)){
                    return  false;
                }
                List<String> platforms = queryParamMap.get("platform");
                if(platforms.size() > 0 && !platforms.contains(platform)){
                    return false;
                }
                List<String> versions = queryParamMap.get("version");
                if (versions.size() > 0 && !versions.contains(version)){
                    return false;
                }
                return true;
            }
        });

        //过滤出来的原始日志，映射为（日期_搜索词 ,用户）的格式
        final JavaPairRDD<String,String> dateKeywordUserRDD = filterRDD.mapToPair(new PairFunction<String, String, String>() {
            private static final long serialVersionUID = 3908094210131573915L;

            @Override
            public Tuple2<String, String> call(String log) throws Exception{
                String [] logSplited = log.split("\t");
                String date = logSplited[0];
                String keyword = logSplited[2];
                String user = logSplited[1];
                String dateKeywordUser = date + "_" + keyword;
                return new Tuple2<String, String>(dateKeywordUser,user);
            }
        });

        //进行分组，获取每天每个搜索词，有哪些用户搜索了（没有去重）
        JavaPairRDD<String,Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();


        //对每天每个搜索词的搜索用户，执行去重操作，获得其uv
        JavaPairRDD<String,Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            private static final long serialVersionUID = 372560656594741262L;

            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers) throws Exception {
                String dateKeyword = dateKeywordUsers._1;
                Iterator<String> users = dateKeywordUsers._2.iterator();

                //对用户进行去重，并统计去重后的数量
                List<String> distinctUsers =new ArrayList<String>();

                while (users.hasNext()){
                    String user = users.next();
                    if (!distinctUsers.contains(user)){
                        distinctUsers.add(user);
                    }
                }
                //获取uv
                long uv = distinctUsers.size();

                return new Tuple2<String, Long>(dateKeyword,uv);
            }
        });

        //将每天每个搜索此的uv数据，转换成DateFrame
        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(new Function<Tuple2<String, Long>, Row>() {
            private static final long serialVersionUID = -2051415150870256771L;

            @Override
            public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
                String date = dateKeywordUv._1.split("_")[0];
                String keyword = dateKeywordUv._1.split("_")[1];
                long uv = dateKeywordUv._2;
                return RowFactory.create(date,keyword,uv);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType,true),
                DataTypes.createStructField("keyword",DataTypes.StringType,true),
                DataTypes.createStructField("uv",DataTypes.LongType,true)
        );
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame dateKeywordUvDF = hct.createDataFrame(dateKeywordUvRowRDD,structType);

        //注册临时表，使用Spark SQL窗口函数，统计每天搜索uv排名和前3的热点搜索词

        dateKeywordUvDF.registerTempTable("daily_keyword_uv");

        DataFrame dailyTop3KeywordDF = hct.sql(""
                + "SELECT date,keyword,uv "
                + "FROM ("
                    + "SELECT "
                        + "date,"
                        + "keyword,"
                        + "uv,"
                        + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
                    + "FROM daily_keyword_uv"
                + ") tmp "
                + "WHERE rank <=3");

        //将DataFrame 转换为RDD， 然后映射，计算出每天的top3索引词的搜索uv总数
        JavaRDD<Row> dailyTop3KeywordRDD = dailyTop3KeywordDF.javaRDD();

        JavaPairRDD<String,String> top3DateKeywordUvRDD = dailyTop3KeywordRDD.mapToPair(new PairFunction<Row, String, String>() {
            private static final long serialVersionUID = -5292367450098984130L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String date = String.valueOf(row.get(0));
                String keyword = String.valueOf(row.get(1));
                Long uv = Long.valueOf(String.valueOf(row.get(2)));
                return new Tuple2<String, String>(date,keyword + "_" + uv);
            }
        });

        JavaPairRDD<String,Iterable<String>> top3DateKeywordsRDD = top3DateKeywordUvRDD.groupByKey();

        JavaPairRDD<Long,String> uvDateKeywordsRDD = top3DateKeywordsRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            private static final long serialVersionUID = -4603314715624792764L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String date = tuple._1;
                Long totalUv = 0L;
                String dateKeywords =date;

                Iterator<String> keywordUvIterator = tuple._2.iterator();
                while (keywordUvIterator.hasNext()){
                    String keywordUv = keywordUvIterator.next();
                    Long uv = Long.valueOf(keywordUv.split("_")[1]);
                    totalUv +=uv;

                    dateKeywords += ',' + keywordUv;
                }
                return new Tuple2<Long, String>(totalUv,dateKeywords);
            }
        });

        //按照每天的总数搜索uv进行倒叙排序
        JavaPairRDD<Long,String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);

        //再次进行映射，将排序后的数据，映射回原始的格式，Iterable<Row>
        JavaRDD<Row> sortRowRDD = sortedUvDateKeywordsRDD.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            private static final long serialVersionUID = -7055917338994268330L;

            @Override
            public Iterable<Row> call(Tuple2<Long, String> tuple2) throws Exception {
                String dateKeywords = tuple2._2;
                String [] dateKeywordsSplited = dateKeywords.split(",");

                String date = dateKeywordsSplited[0];

                List<Row> rows =new ArrayList<Row>();
                rows.add(RowFactory.create(date,
                                            dateKeywordsSplited[1].split("_")[0],
                                            Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
                rows.add(RowFactory.create(date,
                        dateKeywordsSplited[2].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
                rows.add(RowFactory.create(date,
                        dateKeywordsSplited[3].split("_")[0],
                        Long.valueOf(dateKeywordsSplited[3].split("_")[1])));
                return rows;
            }
        });

        //将最终的数据，转换为DataFrame，并保存到hive表中
        DataFrame finalDF = hct.createDataFrame(sortRowRDD,structType);
        finalDF.saveAsTable("daily_top3_keyword_uv");
        sc.close();
    }
}