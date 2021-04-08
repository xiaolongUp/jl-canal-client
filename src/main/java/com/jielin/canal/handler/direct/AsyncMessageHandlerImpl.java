package com.jielin.canal.handler.direct;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.jielin.canal.handler.EntryHandler;

import java.util.concurrent.ExecutorService;

/**
 * @author yxl
 * @description 异步处理接收到client直连的binlog信息，不通过第三方消息中间件
 * @date 2021-03-23 13:50:49
 * @since jdk1.8
 */
@SuppressWarnings("unused")
public class AsyncMessageHandlerImpl extends AbstractDefaultMessageHandler {


    private ExecutorService executor;


    public AsyncMessageHandlerImpl(EntryHandler entryHandler, RowDataHandler<CanalEntry.RowData> rowDataHandler, ExecutorService executor) {
        super(entryHandler, rowDataHandler);
        this.executor = executor;
    }

    @Override
    public void handleMessage(Message message) {
        executor.execute(() -> super.handleMessage(message));
    }
}
