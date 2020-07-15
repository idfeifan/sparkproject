package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITaskDAO;

/**
 * DAO工厂类
 * @Author: Liufeifan
 * @Date: 2020/2/18 15:34
 */
public class DAOFactory {
    public static ITaskDAO getTaskDao(){
        return new TaskDAOImpl();
    }
}
