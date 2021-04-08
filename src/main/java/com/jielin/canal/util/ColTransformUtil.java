package com.jielin.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;

/**
 * @author yxl
 * @description 数据库字段类型转换工具类，
 * 参考阿里 com.alibaba.otter.canal.client.adapter.rdb.support.SyncUtil
 * @date 2021/3/25 3:05 下午
 * @since jdk1.8
 */
public class ColTransformUtil {

    /**
     * 设置 preparedStatement
     *
     * @param type  sqlType {@link CanalEntry.Column.mysqlType_}
     * @param value 值
     */
    public static Object transformCol(int type, Object value) {
        switch (type) {
            case Types.BIT:
            case Types.BOOLEAN:
                if (value instanceof Boolean) {
                    return (Boolean) value;
                } else if (value instanceof String) {
                    return !value.equals("0");
                } else if (value instanceof Number) {
                    return ((Number) value).intValue() != 0;
                } else {
                    return null;
                }
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (value instanceof String) {
                    return (String) value;
                } else if (value == null) {
                    return null;
                } else {
                    return value.toString();
                }
            case Types.TINYINT:
                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                } else if (value instanceof String) {
                    return Byte.parseByte((String) value);
                } else {
                    return null;
                }
            case Types.SMALLINT:
                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                } else if (value instanceof String) {
                    return Short.parseShort((String) value);
                } else {
                    return null;
                }
            case Types.INTEGER:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                } else if (value instanceof String) {
                    return Integer.parseInt((String) value);
                } else {
                    return null;
                }
            case Types.BIGINT:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else if (value instanceof String) {
                    return Long.parseLong((String) value);
                } else {
                    return null;
                }
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (value instanceof BigDecimal) {
                    return (BigDecimal) value;
                } else if (value instanceof Byte) {
                    return ((Byte) value).intValue();
                } else if (value instanceof Short) {
                    return ((Short) value).intValue();
                } else if (value instanceof Integer) {
                    return (Integer) value;
                } else if (value instanceof Long) {
                    return (Long) value;
                } else if (value instanceof Float) {
                    return new BigDecimal((float) value);
                } else if (value instanceof Double) {
                    return new BigDecimal((double) value);
                } else if (value != null) {
                    return new BigDecimal(value.toString());
                } else {
                    return null;
                }
            case Types.REAL:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else if (value instanceof String) {
                    return Float.parseFloat((String) value);
                } else {
                    return null;
                }
            case Types.FLOAT:
            case Types.DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else if (value instanceof String) {
                    return Double.parseDouble((String) value);
                } else {
                    return null;
                }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                if (value instanceof Blob) {
                    return (Blob) value;
                } else if (value instanceof byte[]) {
                    return (byte[]) value;
                } else if (value instanceof String) {
                    return ((String) value).getBytes(StandardCharsets.ISO_8859_1);
                } else {
                    return null;
                }
            case Types.CLOB:
                if (value instanceof Clob) {
                    return (Clob) value;
                } else if (value instanceof byte[]) {
                    return (byte[]) value;
                } else if (value instanceof String) {
                    return (String) value;
                } else {
                    return null;
                }
            case Types.DATE:
                if (value instanceof Date) {
                    return (Date) value;
                } else if (value instanceof java.util.Date) {
                    return new Date(((java.util.Date) value).getTime());
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        java.util.Date date = Util.parseDate(v);
                        if (date != null) {
                            return new Date(date.getTime());
                        } else {
                            return null;
                        }
                    } else {
                        return value;
                    }
                } else {
                    return null;
                }
            case Types.TIME:
                if (value instanceof Time) {
                    return (Time) value;
                } else if (value instanceof java.util.Date) {
                    return new Time(((java.util.Date) value).getTime());
                } else if (value instanceof String) {
                    String v = (String) value;
                    java.util.Date date = Util.parseDate(v);
                    if (date != null) {
                        return new Time(date.getTime());
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            case Types.TIMESTAMP:
                if (value instanceof Timestamp) {
                    return (Timestamp) value;
                } else if (value instanceof java.util.Date) {
                    return new Timestamp(((java.util.Date) value).getTime());
                } else if (value instanceof String) {
                    String v = (String) value;
                    if (!v.startsWith("0000-00-00")) {
                        java.util.Date date = Util.parseDate(v);
                        if (date != null) {
                            return new Timestamp(date.getTime());
                        } else {
                            return null;
                        }
                    } else {
                        return value;
                    }
                } else {
                    return null;
                }
            default:
                return value;
        }
    }
}
