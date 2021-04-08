package com.jielin.canal.handler.direct;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetSocketAddress;

/**
 * @author yxl
 * @description 直连canal-server去消费数据
 * @date 2021-03-30 13:12:39
 * @since jdk1.8
 */
public class DirectCanalClient implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(DirectCanalClient.class);

    private final static int BATCH_SIZE = 1000;

    private SyncMessageHandlerImpl syncMessageHandler;

    public DirectCanalClient(SyncMessageHandlerImpl syncMessageHandler) {
        this.syncMessageHandler = syncMessageHandler;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111), "example", "", "");
        try {
            //打开连接
            connector.connect();
            //订阅数据库表,全部表
            connector.subscribe("test_case\\..*");
            //回滚到未进行ack的地方，下次fetch的时候，可以从最后一个没有ack的地方开始拿
            connector.rollback();
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(BATCH_SIZE);
                //获取批量ID
                long batchId = message.getId();
                //获取批量的数量
                int size = message.getEntries().size();
                //如果没有数据
                if (batchId == -1 || size == 0) {
                    try {
                        //线程休眠2秒
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    //如果有数据,处理数据
                    syncMessageHandler.handleMessage(message);
                }
                //进行 batch id 的确认。确认之后，小于等于此 batchId 的 Message 都会被确认。
                connector.ack(batchId);
            }
        } catch (Exception e) {
            logger.error("数据同步异常：", e);
        } finally {
            connector.disconnect();
        }
    }
}