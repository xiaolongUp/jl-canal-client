package com.jielin.canal.handler.direct;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.jielin.canal.handler.EntryHandler;

/**
 * @author yxl
 * @description rowData数据处理
 * @date 2021-03-23 14:07:32
 * @since jdk1.8
 */
public interface RowDataHandler<T> {


    <R> void handlerRowData(T t, EntryHandler<R> entryHandler, CanalEntry.EventType eventType, String tableName) throws Exception;
}
