package com.jielin.canal.handler.kafka;

import com.jielin.canal.bean.Dml;
import com.jielin.canal.bean.SingleDml;
import com.jielin.canal.handler.EntryHandler;
import com.jielin.canal.handler.MessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author yxl
 * @description kafka处理接收到的消息
 * @date 2021/3/30 1:08 下午
 * @since jdk1.8
 */

@Component
public class SyncKafkaMessageHandler implements MessageHandler<Dml> {

    @Autowired
    private EntryHandler entryHandler;

    @Autowired
    private SingleDmlDataHandler<SingleDml> singleDmlDataHandler;

    @Override
    @Transactional(rollbackFor = Exception.class)//事物必须加上，防止异常数据未回滚
    public void handleMessage(Dml dml) {
        try {
            //当为ddl语句时直接执行
            if (dml.getIsDdl()) {
                entryHandler.ddl(dml.getSql());
            } else {
                List<Map<String, Object>> data = dml.getData();
                if (CollectionUtils.isEmpty(data)) {
                    return;
                }
                List<SingleDml> singleDmls = SingleDml.dml2SingleDmls(dml);
                for (SingleDml singleDml : singleDmls) {
                    singleDmlDataHandler.handlerSingleDmlData(singleDml, entryHandler, dml.getType(), dml.getSqlType());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("parse event has an error , data:" + dml.toString(), e);
        }
    }
}
