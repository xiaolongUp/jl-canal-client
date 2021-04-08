package com.jielin.canal.handler.direct;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.jielin.canal.handler.EntryHandler;
import com.jielin.canal.handler.MessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * @author yxl
 * @description binlog直连消息处理器，处理消息类型为{@link Message}
 * @date 2021-03-23 13:59:39
 * @since jdk1.8
 */
public abstract class AbstractDefaultMessageHandler implements MessageHandler<Message> {

    @Autowired
    private EntryHandler entryHandler;

    @Autowired
    private RowDataHandler<CanalEntry.RowData> rowDataHandler;


    public AbstractDefaultMessageHandler(EntryHandler entryHandler, RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        this.entryHandler = entryHandler;
        this.rowDataHandler = rowDataHandler;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)//事物必须加上，防止异常数据未回滚
    public void handleMessage(Message message) {
        //三条消息 TRANSACTIONBEGIN，{insert|update|delete}，TRANSACTIONEND
        List<CanalEntry.Entry> entries = message.getEntries();
        for (CanalEntry.Entry entry : entries) {
            //处理的binlog日志类型为row，这个都是事先设置好的了
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                //开启/关闭事务的实体类型，跳过
                continue;
            }
            if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                try {
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    //ddl语句直接执行
                    if (rowChange.getIsDdl()) {
                        entryHandler.ddl(rowChange.getSql());
                    }
                    //dml语句需要处理
                    else {
                        for (CanalEntry.RowData rowData : rowDataList) {
                            rowDataHandler.handlerRowData(rowData, entryHandler, eventType, entry.getHeader().getTableName());
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }
            }
        }
    }
}
