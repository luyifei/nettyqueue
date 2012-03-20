package com.yongboy.mq;

import org.apache.log4j.Logger;

/**
 * 简单队列启动入口
 * 
 * @author yongboy
 * @time 2012-3-19
 * @version 1.0
 */
public class QueueServer {
	private static final Logger log = Logger.getLogger(QueueServer.class);

	public static void main(String[] args) {
		final QueueService queueService = new RedisService();
		final int port = 8080;

		QueueDaemon queueDaemon = new QueueDaemon(port, queueService);

		log.info("QueueServer start with port 8080 start now ~");
		queueDaemon.start();
	}
}