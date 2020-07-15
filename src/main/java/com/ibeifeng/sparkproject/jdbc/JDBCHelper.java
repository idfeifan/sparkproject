package com.ibeifeng.sparkproject.jdbc;

import com.ibeifeng.sparkproject.Constants;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;


/**
 * Jbdbc 辅助组件
 * @Author: Liufeifan
 * @Date: 2020/2/12 16:49
 */
public class JDBCHelper {
    //第一步：在静态代码块中加载数据库驱动
    static {
        String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //第二步：实现JDBCHelper的单例化
    private static JDBCHelper instance =null;
    /**
     * 获取单例
     */
    public static JDBCHelper getInstance(){
        if (instance == null){
            synchronized (JDBCHelper.class){
                if (instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }
    //数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();
    /**
     * 第三步：实现单例的过程中，创建唯一的数据连接池
     * 私有化构造方法
     */
    private JDBCHelper() {
        //首先，获取数据库连接池的大小，就是说，数据库连接池中要存放多个数据库连接
        //这个，可以通过在配置文件中配置的方式，来灵活的设定
        int datasourceSize = ConfigurationManager.getInteger(
                Constants.JDBC_DATASOURCE_SIZE);
        for(int i = 0;i<datasourceSize;i++){
            String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(url,user,password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 第四步：提供获取数据库连接的方法
     * 假如连接用光，编写一个简单的等待机制，去等待
     */
    public synchronized Connection getConnnection(){
        while(datasource.size()==0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }
    /**
     * 第五步：开发增删改查的方法
     * 1/执行增删该sql语句的方法
     * 2.执行查询SQL语句的方法
     * 3.批量执行SQL语句的方法
     */

    /**
     *执行增删该SQL 语句
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql,Object[] params){
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt  = null;
        try {
            conn = getConnnection();
            pstmt = conn.prepareStatement(sql);
            for (int i =0; i <params.length;i++){
                pstmt.setObject(i + 1, params[i]);
            }
            rtn = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (conn != null){
                datasource.push(conn);
            }  //将走连接池中的连接数还回
        }
        return rtn;
    }
    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql,Object[] params ,
                             QueryCallback callback)  {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = getConnnection();
            pstmt = conn.prepareStatement(sql);
            for(int i = 0 ; i < params.length;i++){
                pstmt.setObject(i + 1 , params[i]);
            }
            rs = pstmt.executeQuery();
            callback.process(rs);
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if(conn!= null){
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行SQL语句
     * 比如100条，1000条
     * @param sql
     * @param paramslist
     * @return 每条sql语句影响的行数
     */
    public int[] executeBatch(String sql , List<Object[]> paramslist){
        int [] rtn = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try{
            conn = getConnnection();
            //第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            //第二步：使用PreparedStatement.addBatch加入批量sql参数。
            for(Object[] params :paramslist){
                for(int i = 0; i <params.length;i++){
                    pstmt.setObject(i + 1 ,params[i]);
                }
                pstmt.addBatch();
            }
            // 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
            rtn = pstmt.executeBatch();

            // 最后一步：使用Connection对象，提交批量的SQL语句
            conn.commit();
        }catch (Exception e){
            e.printStackTrace();
        }
        return rtn;
    }




    public static interface QueryCallback{
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
