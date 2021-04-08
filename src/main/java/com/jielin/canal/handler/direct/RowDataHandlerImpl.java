package com.jielin.canal.handler.direct;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.jielin.canal.factory.IModelFactory;
import com.jielin.canal.handler.EntryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author yxl
 * @description 按照每行的数据去处理数据的保存，
 * for example：当update多条数据时，此处的处理会按照的update的行数去处理每行的dml语句
 * @date 2021-03-25 11:52:24
 * @since jdk1.8
 */
@Component
public class RowDataHandlerImpl implements RowDataHandler<CanalEntry.RowData> {

    @Autowired
    private IModelFactory<List<CanalEntry.Column>> modelFactory;

    @Override
    public <R> void handlerRowData(CanalEntry.RowData rowData, EntryHandler<R> entryHandler, CanalEntry.EventType eventType, String tableName) throws Exception {

        if (entryHandler != null) {
            switch (eventType) {
                case INSERT:
                    R object = modelFactory.newInstance(entryHandler, rowData.getAfterColumnsList());
                    entryHandler.insert(object, tableName);
                    break;
                case UPDATE:
                    Set<String> updateColumnSet = rowData.getAfterColumnsList().stream().filter(CanalEntry.Column::getUpdated)
                            .map(CanalEntry.Column::getName).collect(Collectors.toSet());
                    R before = modelFactory.newInstance(entryHandler, rowData.getBeforeColumnsList(), updateColumnSet);
                    R after = modelFactory.newInstance(entryHandler, rowData.getAfterColumnsList());
                    entryHandler.update(before, after, tableName);
                    break;
                case DELETE:
                    R o = modelFactory.newInstance(entryHandler, rowData.getBeforeColumnsList());
                    entryHandler.delete(o, tableName);
                    break;
                default:
                    break;
            }
        }
    }
}
