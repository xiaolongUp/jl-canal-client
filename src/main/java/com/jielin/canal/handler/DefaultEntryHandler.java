package com.jielin.canal.handler;


import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Param;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * @author yxl
 * @description 获取到map 对象后转换成sql，使用jooq执行 sql，ddl语句直接执行
 * @date 2021-03-23 14:02:08
 * @since jdk1.8
 */
@Component
public class DefaultEntryHandler implements EntryHandler<Map<String, Object>> {


    @Resource
    private DSLContext dsl;


    private Logger logger = LoggerFactory.getLogger(DefaultEntryHandler.class);


    @Override
    public void insert(Map<String, Object> map, String tableName) {
        logger.info("增加 {}", map);
        List<Field<Object>> fields = map.keySet().stream().map(DSL::field).collect(Collectors.toList());
        List<Param<Object>> values = map.values().stream().map(DSL::value).collect(Collectors.toList());
        int execute = dsl.insertInto(table(tableName)).columns(fields).values(values).execute();
        logger.info("执行结果 {}", execute);
    }

    @Override
    public void update(Map<String, Object> before, Map<String, Object> after, String tableName) {
        logger.info("修改 before {}", before);
        logger.info("修改 after {}", after);
        Map<Field<Object>, Object> map = after.entrySet().stream().filter(entry -> before.get(entry.getKey()) != null)
                .collect(Collectors.toMap(entry -> field(entry.getKey()), Map.Entry::getValue));
        // 如果有修改主键的情况
        if (before.containsKey("id")) {
            dsl.update(table(tableName)).set(map).where(field("id").eq(before.get("id"))).execute();
        } else {
            dsl.update(table(tableName)).set(map).where(field("id").eq(after.get("id"))).execute();
        }
    }

    @Override
    public void delete(Map<String, Object> map, String tableName) {
        logger.info("删除 {}", map);
        dsl.delete(table(tableName)).where(field("id").eq(map.get("id"))).execute();
    }

    @Override
    public void ddl(String ddl) {
        logger.info("ddl操作：{}", ddl);
        dsl.execute(ddl);
    }

    @Override
    public void truncate(String tableName){
        dsl.truncate(tableName);
    }
}
