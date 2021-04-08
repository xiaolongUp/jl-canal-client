package com.jielin.canal.handler.direct;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.jielin.canal.handler.EntryHandler;
import org.springframework.stereotype.Component;


/**
 * @author yxl
 * @description 同步处理接收的信息client直连的binlog信息，不通过第三方消息中间件
 * @date 2021-03-23 13:50:21
 * @since jdk1.8
 */
@Component
public class SyncMessageHandlerImpl extends AbstractDefaultMessageHandler {

    public SyncMessageHandlerImpl(EntryHandler entryHandler, RowDataHandler<CanalEntry.RowData> rowDataHandler) {
        super(entryHandler, rowDataHandler);
    }

    @Override
    public void handleMessage(Message message) {
        super.handleMessage(message);
    }

}
