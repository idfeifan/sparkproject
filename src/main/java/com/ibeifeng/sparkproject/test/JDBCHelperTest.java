package com.ibeifeng.sparkproject.test;

import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * 辅助组件测试类
 * @Author: Liufeifan
 * @Date: 2020/2/14 14:38
 */
public class JDBCHelperTest {
    public static void main(String[] args) throws Exception {
        //获取JDBCHelper单例
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        //测试普通的增删改语句
//        Object[] a = {"王二",28};
//        jdbcHelper.executeUpdate("insert into user_test(name,age) values(?,?)",
//                a);
        //测试查询语句
//        final Map<String,Object> userTest = new HashMap<String,Object>();
//
//        jdbcHelper.executeQuery("select name,age from user_test where adress =?",
//                new Object[]{1},
//                new JDBCHelper.QueryCallback() {
//                    @Override
//                    public void process(ResultSet rs) throws Exception {
//                        if(rs.next()){
//                            String name = rs.getString(1);
//                            int age = rs.getInt(2);
//
//                            //匿名内部类的一个重要知识，如果要访问外部类中的一些成员
//                            //那么，必须将变量声明为final类型，才可以访问
//                            userTest.put("name",name);
//                            userTest.put("age",age);
//                        }
//                    }
//                });
//        System.out.println(userTest.get("name")+": " + userTest.get("age"));
    //测试批量执行SQL语句
        String sql = "insert into user_test(name,age) values(?,?)";

        List<Object[]>paramList = new ArrayList<Object[]>();
        paramList.add(new Object[]{"李四",30});
        paramList.add(new Object[]{"缺五",40});
        int[]a = jdbcHelper.executeBatch(sql,paramList);
        for(int i= 0; i<a.length;i++){
            System.out.println(a[i]);
        }
    }

}
