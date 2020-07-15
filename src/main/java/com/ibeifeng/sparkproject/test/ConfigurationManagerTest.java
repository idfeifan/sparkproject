package com.ibeifeng.sparkproject.test;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
/**
 * @Author: Liufeifan
 * @Date: 2020/2/12 14:32
 */
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String testkey1 = ConfigurationManager.getProperty("key1");
        String testkey2 = ConfigurationManager.getProperty("key2");
        System.out.println(testkey1);
        System.out.println(testkey2);
    }
}
