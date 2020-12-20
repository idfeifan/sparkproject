package com.ibeifeng.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.ibeifeng.sparkproject.Constants;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.impl.DAOFactory;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.test.MockData;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import com.ibeifeng.sparkproject.util.ValidUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.Iterator;


/**
 * 用户访问session分析spark作业＋
 * @Author: Liufeifan
 * @Date: 2020/2/18 15:45
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        args = new String[]{"2"};
        //构建spark上线文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext  = getSQLContext(sc.sc());
        //生成模拟数据
        mockData(sc,sqlContext);
        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDao();
        //首先查询出来指定的任务，并获取任务的查询参数
        long taskid = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskid);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //进行session粒度的数据聚合。
        // 首先从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext,taskParam);
        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
        // 与用户信息数据，进行join
        // 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
        //到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional)>

        JavaPairRDD<String, String> sessionid2AggrInfoRDD =
                aggregateBySession(sqlContext, actionRDD);
        System.out.println(sessionid2AggrInfoRDD.count());
        for(Tuple2<String, String> tuple :sessionid2AggrInfoRDD.take(10)){
            System.out.println(tuple);
        }
        //接着，就要正对session粒度的聚合数据，按照使用着制定的筛选参数进行数据过滤
        JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD =
                filterSession(sessionid2AggrInfoRDD,taskParam);

        System.out.println(filteredSessionid2AggrInfoRDD.count());
        System.out.println(filteredSessionid2AggrInfoRDD.collect());
        sc.close();
    }
    /**
     * 获取SQLContext
     * 如果是本地测试环境的话，那么就生成sqlcontext对象
     * 如果是服务器测试环境，那么就生成HiveContext对象
     */
    private static SQLContext getSQLContext(SparkContext sc){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            return new SQLContext(sc);
        }else {
            return new HiveContext(sc);
        }
    }
    /**
     * 生成模拟数据，（只有本地模式才会去生成模拟数据）
     * @param sc
     * @param sqlContext
     */
    private static void mockData(JavaSparkContext sc, SQLContext sqlContext){
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local){
            MockData.mock(sc,sqlContext);
        }
    }

    /**
     * 获取指定日期范围内的用户访问数据
     * @param sqlContext
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(
            SQLContext sqlContext,JSONObject taskParam){
        String  startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam,Constants.PARAM_EDN_DATE);
        String sql = "select * "
                +"from user_visit_action "
                +"where date>= '"+startDate+"' "
                +"and date<= '" + endDate+"'";
        DataFrame actionDF = sqlContext.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 对行为数据session粒度进行聚合
     * @param actionRDD 行为数据RDD
     * @param sqlContext
     * @return session 粒度聚合数据
     */
     private static JavaPairRDD<String,String> aggregateBySession(
             SQLContext sqlContext, JavaRDD<Row> actionRDD){
        JavaPairRDD<String,Row> sessionid2ActionRDD = actionRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {

                    return new Tuple2<String, Row>(row.getString(2),row);
            }
        });
        //对行为数据按session粒度进行分组
         JavaPairRDD<String,Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

         //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合
         //到此为止，获取的数据格式，如下，<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long,String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    private static final long serialVersionUID = -4377613965796209349L;

                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tup2)
                            throws Exception {
                        String sessionid = tup2._1;
                        Iterator<Row> iterator = tup2._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
                        Long userid = null;

                        //遍历session所有的访问行为
                        while(iterator.hasNext()){
                            //提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null){
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId  = row.getLong(6);

                            // 实际上这里要对数据说明一下
                            // 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
                            // 其实，只有搜索行为，是有searchKeyword字段的
                            // 只有点击品类的行为，是有clickCategoryId字段的
                            // 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
                            // 我们决定是否将搜索词或点击品类id拼接到字符串中去
                            // 首先要满足：不能是null值
                            // 其次，之前的字符串中还没有搜索词或者点击品类id

                            if(StringUtils.isNotEmpty(searchKeyword)) {
                                if(!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if(clickCategoryId != null) {
                                if(!clickCategoryIdsBuffer.toString().contains(
                                        String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }
                        }
                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }
            });
        //查询所有用户数据，并映射成<userid,Row>的格式
         String sql  = "select * from user_info";
         final JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
         JavaPairRDD<Long,Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
             @Override
             public Tuple2<Long, Row> call(Row row) throws Exception {
                 return new Tuple2<Long, Row>(row.getLong(0),row);
             }
         });

         //将session粒度聚合数据，与用户信息进行join
         JavaPairRDD<Long,Tuple2<String,Row>> userid2FullInfoRDD =
                 userid2PartAggrInfoRDD.join(userid2InfoRDD);

         //对join起来的数据进行拼接，并且返回<sessionid,fullAggrinfo>格式的数据
         JavaPairRDD<String,String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
                 new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                     private static final long serialVersionUID = -1438731832746597737L;

                     @Override
                     public Tuple2<String, String> call(
                             Tuple2<Long, Tuple2<String, Row>> tuple)
                             throws Exception {
                         String partAggrInfo = tuple._2._1;
                         Row userInfoRow = tuple._2._2;

                         String sessionid = StringUtils.getFieldFromConcatString(
                                 partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                         int age = userInfoRow.getInt(3);
                         String professional = userInfoRow.getString(4);
                         String city = userInfoRow.getString(5);
                         String sex = userInfoRow.getString(6);

                         String fullAggrInfo = partAggrInfo +"|"
                                 +Constants.FIELD_AGE + "=" + age + "|"
                                 +Constants.FIELD_PROFESSIONAL + "=" +professional + "|"
                                 +Constants.FIELD_CITY + "=" + city + "|"
                                 +Constants.FIELD_SEX + "=" + sex;
                         return new Tuple2<String, String>(sessionid,fullAggrInfo);
                     }
                 });
        return sessionid2FullAggrInfoRDD;
     }

    /**
     * 过滤操作
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @return
     */
     private static JavaPairRDD<String,String> filterSession(
             JavaPairRDD<String, String> sessionid2AggrInfoRDD,
             final JSONObject taskParam){
        //为了使用我们后面的Valieutils, 所以，首先将所有的筛选参数拼接成一个连接串
        //此外，这里其实大家不要觉得多次一举
        //其实我们是给后面的性能优化埋下一个伏笔

         String startAge =  ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
         String endAge = ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
         String professional = ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS);
         String cities  = ParamUtils.getParam(taskParam,Constants.PARAM_CITIES);
         String sex = ParamUtils.getParam(taskParam,Constants.PARAM_SEX);
         String keywords = ParamUtils.getParam(taskParam,Constants.PARAM_KEYWORDS);
         String categroyIds = ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS);

         String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|":"")
                 + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|":"")
                 + (professional != null ? Constants.PARAM_PROFESSIONALS + "=" + professional + "|":"")
                 + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|":"")
                 + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|":"")
                 + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|":"")
                 + (categroyIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categroyIds + "|":"");
         if(_parameter.endsWith("\\|")){
             _parameter = _parameter.substring(0,_parameter.length() - 1);
         }
         final String parameter = _parameter;
         // 根据筛选参数进行过滤
         JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(

                 new Function<Tuple2<String,String>, Boolean>() {

                     private static final long serialVersionUID = 1L;

                     @Override
                     public Boolean call(Tuple2<String, String> tuple) throws Exception {
                         // 首先，从tuple中，获取聚合数据
                         String aggrInfo = tuple._2;

                         // 接着，依次按照筛选条件进行过滤
                         // 按照年龄范围进行过滤（startAge、endAge）
                         if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
                                 parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                             return false;
                         }

                         // 按照职业范围进行过滤（professionals）
                         // 互联网,IT,软件
                         // 互联网
                         if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
                                 parameter, Constants.PARAM_PROFESSIONALS)) {
                             return false;
                         }

                         // 按照城市范围进行过滤（cities）
                         // 北京,上海,广州,深圳
                         // 成都
                         if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
                                 parameter, Constants.PARAM_CITIES)) {
                             return false;
                         }

                         // 按照性别进行过滤
                         // 男/女
                         // 男，女
                         if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
                                 parameter, Constants.PARAM_SEX)) {
                             return false;
                         }

                         // 按照搜索词进行过滤
                         // 我们的session可能搜索了 火锅,蛋糕,烧烤
                         // 我们的筛选条件可能是 火锅,串串香,iphone手机
                         // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                         // 任何一个搜索词相当，即通过
                         if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
                                 parameter, Constants.PARAM_KEYWORDS)) {
                             return false;
                         }

                         // 按照点击品类id进行过滤
                         if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
                                 parameter, Constants.PARAM_CATEGORY_IDS)) {
                             return false;
                         }

                         return true;
                     }

                 });
         return filteredSessionid2AggrInfoRDD;
     }

}
