package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

/**
 * @Author: Liufeifan
 * @Date: 2020/2/18 15:14
 */
public interface ITaskDAO {
    /**
     * 根据主键查询任务
     */
    Task findById(long taskid);
}
