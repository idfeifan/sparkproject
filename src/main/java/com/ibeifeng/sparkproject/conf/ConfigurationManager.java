package com.ibeifeng.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 */
public class ConfigurationManager {
    //设置私有对象防止外界代码随意更改
    private static Properties prop = new Properties();

    /**
     * 静态代码快
     */
    static {
        //加载文件输入流，读取配置文件信息
        InputStream in = ConfigurationManager.class
                .getClassLoader().getResourceAsStream("my.properties");

        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static String getProperty(String key){
        return prop.getProperty(key);
    }
    public static Integer getInteger(String key){
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e){
            System.out.println("不是数字，不能转换");
        }
        return 0;
    }
    public static Boolean getBoolean(String key){
        String value = getProperty(key);
        try{
            return Boolean.valueOf(value);
        } catch (Exception e){
            System.out.println("Boolean 转化失败");
        }
        return false;
    }
}
