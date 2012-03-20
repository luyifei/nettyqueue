package com.yongboy.mq;

/**
 * 队列服务接口
 * 
 * @author yongboy
 * @time 2012-3-20
 * @version 1.0
 */
public interface QueueService {

	/**
	 * 设置数据
	 * 
	 * @param conn
	 */
	Long push(String key, String value);

	/**
	 * 获取队列元素
	 * 
	 * @param conn
	 */
	String pop(String key);
}