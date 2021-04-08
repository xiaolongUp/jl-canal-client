package com.jielin.canal.handler.kafka;

import com.jielin.canal.handler.EntryHandler;

import java.util.Map;

public interface SingleDmlDataHandler<T> {

    <R> void handlerSingleDmlData(T t, EntryHandler<R> entryHandler, String eventType, Map<String, Integer> sqlType) throws Exception;

}
