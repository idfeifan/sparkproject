package com.ibeifeng;
import java.sql.*;


public class test {
    private  static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException {
        try{
            Class.forName(driverName);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection("jdbc:hive://192.168.1.5:10000/ods","root","123456");
        Statement stmt = conn.createStatement();
        String tableName  = "test2";
        stmt.executeQuery("drop table test2");
        ResultSet res = stmt.executeQuery("create table test2( key int ,value String)");

        // show tables;
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running :" + sql);
        res = stmt.executeQuery(sql);
        if (res.next()){
            System.out.println(res.getString(1));
        }


        //desc table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res  = stmt.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getString(1) + '\t' + res.getString(2));
        }

        //
    }
}
