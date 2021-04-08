package com.jielin.canal.factory;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.jielin.canal.handler.EntryHandler;
import com.jielin.canal.util.ColTransformUtil;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yxl
 * @description 构造数据库的存储对象，此处是通用设计，返回的对象都为map数据
 * @date 2021-03-25 10:28:14
 * @since jdk1.8
 */
@Component
public class EntryColumnModelFactory implements IModelFactory<List<CanalEntry.Column>> {


    /**
     * 需要插入的字段的处理
     */
    @Override
    public <R> R newInstance(EntryHandler entryHandler, List<CanalEntry.Column> columns) throws Exception {
        //Collectors.toMap 存入 null会报错，改成下面的方式
        // https://stackoverflow.com/questions/24630963/nullpointerexception-in-collectors-tomap-with-null-entry-values
        Map<String, Object> map = columns.stream().collect(
                HashMap::new, (m, v) -> m.put(v.getName(),
                        //类型转成我们需要的类型
                        v.getIsNull() ? null : ColTransformUtil.transformCol(v.getSqlType(), v.getValue())),
                HashMap::putAll);
        return (R) map;
    }

    /**
     * 需要更新的字段做更新处理，既需要将更新前字段和更新后的字段取交集
     */
    @Override
    public <R> R newInstance(EntryHandler entryHandler, List<CanalEntry.Column> columns, Set<String> updateColumn) throws Exception {
        Map<String, Object> map = columns.stream().filter(column -> updateColumn.contains(column.getName()))
                .collect(
                        HashMap::new, (m, v) -> m.put(v.getName(),
                                //类型转成我们需要的类型
                                v.getIsNull() ? null : ColTransformUtil.transformCol(v.getSqlType(), v.getValue())),
                        HashMap::putAll);
        return (R) map;
    }

}
