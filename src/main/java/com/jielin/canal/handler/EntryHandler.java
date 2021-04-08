package com.jielin.canal.handler;

/**
 * @author yxl
 * @description 默认增删该查
 * @date 2021-03-23 14:05:01
 * @since jdk1.8
 */
public interface EntryHandler<T> {


    default void insert(T t, String tableName) {

    }


    default void update(T before, T after, String tableName) {

    }


    default void delete(T t, String tableName) {

    }

    default void ddl(String ddl){

    }

    default void truncate(String tableName){

    }
}
