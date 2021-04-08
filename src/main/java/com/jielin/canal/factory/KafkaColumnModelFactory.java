package com.jielin.canal.factory;

import com.jielin.canal.bean.SingleDml;
import com.jielin.canal.util.ColTransformUtil;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author yxl
 * @description TODO
 * @date 2021/3/30 1:28 下午
 * @since jdk1.8
 */
@Component
public class KafkaColumnModelFactory {

    public <R> R newInstance(SingleDml singleDml, Map<String, Integer> sqlType) throws Exception {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> data = singleDml.getData();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object v = entry.getValue();
            if (v != null) {
                v = ColTransformUtil.transformCol(sqlType.get(entry.getKey()), v);
            }
            map.put(entry.getKey(), v);
        }
        return (R) map;
    }

    public <R> R newInstance(SingleDml singleDml, Set<String> updateColumn, Map<String, Integer> sqlType) throws Exception {

        Map<String, Object> data = singleDml.getOld();
        Map<String, Object> map = new HashMap<>();

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (updateColumn.contains(entry.getKey())) {
                Object v = entry.getValue();
                if (v != null) {
                    v = ColTransformUtil.transformCol(sqlType.get(entry.getKey()), v);
                }
                map.put(entry.getKey(), v);
            }
        }
        return (R) map;
    }
}
