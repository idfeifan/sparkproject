package com.ibeifeng.sparkproject;

/**
 * 常量接口
 * 
 * @Author: Liufeifan
 * @Date: 2020/2/12 16:53
 */
public interface Constants {
    /**
     * 项目相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String SPARK_LOCAL =  "spark.local";

    /**
     * saprk 相关常量
     */
    String SPARK_APP_NAME_SESSION ="UserVisitSessionAnalyzeSpark";
    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryids";
    String FIELD_AGE="age";
    String FIELD_PROFESSIONAL="professional";
    String FIELD_CITY="city";
    String FIELD_SEX ="sex";
    /**
     * 任务相关常量
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_EDN_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_IDS = "categoryIds";
}
