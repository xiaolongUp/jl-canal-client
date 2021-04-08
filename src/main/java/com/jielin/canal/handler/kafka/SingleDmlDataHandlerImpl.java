package com.jielin.canal.handler.kafka;

import com.jielin.canal.bean.SingleDml;
import com.jielin.canal.factory.KafkaColumnModelFactory;
import com.jielin.canal.handler.EntryHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yxl
 * @description kafka接收对象处理
 * @date 2021/3/30 1:22 下午
 * @since jdk1.8
 */
@Component
public class SingleDmlDataHandlerImpl implements SingleDmlDataHandler<SingleDml> {

    @Autowired
    private KafkaColumnModelFactory modelFactory;

    @Override
    public <R> void handlerSingleDmlData(SingleDml singleDml, EntryHandler<R> entryHandler, String eventType, Map<String, Integer> sqlType) throws Exception {
        if (entryHandler != null) {
            if (eventType != null && eventType.equalsIgnoreCase("INSERT")) {
                R object = modelFactory.newInstance(singleDml, sqlType);
                entryHandler.insert(object, singleDml.getTable());
            } else if (eventType != null && eventType.equalsIgnoreCase("UPDATE")) {
                Set<String> updateColumnSet = new HashSet<>(singleDml.getOld().keySet());
                R before = modelFactory.newInstance(singleDml, updateColumnSet, sqlType);
                R after = modelFactory.newInstance(singleDml, sqlType);
                entryHandler.update(before, after, singleDml.getTable());
            } else if (eventType != null && eventType.equalsIgnoreCase("DELETE")) {
                R o = modelFactory.newInstance(singleDml, sqlType);
                entryHandler.delete(o, singleDml.getTable());
            } else if (eventType != null && eventType.equalsIgnoreCase("TRUNCATE")) {
                entryHandler.truncate(singleDml.getTable());
            }
        }
    }
}
