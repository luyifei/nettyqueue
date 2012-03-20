package com.yongboy.mq;

import java.util.List;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

/**
 * 模拟队列处理器<br/>
 * 这里仅仅使用Jedis进行连接，轻量一些
 * 
 * @author yongboy
 * @time 2012-3-20
 * @version 1.0
 */
public class QueueHandler {
	private static final Logger log = Logger.getLogger(QueueHandler.class);

	public static void main(String[] args) {
		final Jedis jedis = RedisService.getJedis();
		final int timeout = 100000;
		final String queueName = "demo";

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("prepare to close the jedis instance");
				if (jedis != null) {
					RedisService.closeJedis(jedis);
				}
			}
		});

		for (;;) {
			log.debug("entry the block method now ...");
			List<String> results = jedis.blpop(timeout, queueName);
			if (results == null || results.isEmpty()) {
				log.debug("with timeout : " + timeout
						+ " get empty list. will continue now ...");
				continue;
			}

			log.debug("with timeout : " + timeout + " get not empty list : ");
			for (String result : results) {
				if (result.equals(queueName))
					continue;

				log.debug("consume message : " + result);
			}
		}
	}
}