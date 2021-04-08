package com.jielin.canal.handler;

/**
 * @author yxl
 * @description 默认消息处理接口
 * @date 2021-03-23 14:05:43
 * @since jdk1.8
 */
public interface MessageHandler<T> {


     void handleMessage(T t);
}
