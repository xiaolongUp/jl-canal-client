package com.jielin.canal.factory;


import com.jielin.canal.handler.EntryHandler;

import java.util.Set;

/**
 * @author yxl
 * @description 构造需要存储到数据库对象的factory
 * @date 2021-03-23 14:18:58
 * @since jdk1.8
 */
public interface IModelFactory<T> {


    /**
     * insert语句插入map的构造
     */
    <R> R newInstance(EntryHandler entryHandler, T t) throws Exception;


    /**
     * update语句返回的map构造
     */
    default <R> R newInstance(EntryHandler entryHandler, T t, Set<String> updateColumn) throws Exception {
        return null;
    }
}
