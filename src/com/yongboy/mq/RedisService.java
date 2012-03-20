package com.yongboy.mq;

import java.util.ResourceBundle;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 连接和使用数据库资源的工具类
 * 
 * @author yongboy
 * @time 2012-3-19
 * @version 1.0
 */
public class RedisService implements QueueService {
	private static JedisPool pool;

	static {
		ResourceBundle bundle = ResourceBundle.getBundle("redis");
		if (bundle == null) {
			throw new IllegalArgumentException(
					"cannot find the SignVerProp.properties");
		}

		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxActive(Integer.valueOf(bundle
				.getString("redis.pool.maxActive")));
		config.setMaxIdle(Integer.valueOf(bundle
				.getString("redis.pool.maxIdle")));
		config.setMaxWait(Integer.valueOf(bundle
				.getString("redis.pool.maxWait")));

		pool = new JedisPool(config, bundle.getString("redis.server"),
				Integer.valueOf(bundle.getString("redis.port")));
	}

	/**
	 * 从连接池获取
	 * 
	 * @author yongboy
	 * @time 2012-3-20
	 * 
	 * @return
	 */
	public static Jedis getJedis() {
		return pool.getResource();
	}

	/**
	 * 返回给连接池
	 * 
	 * @author yongboy
	 * @time 2012-3-20
	 * 
	 * @param jedis
	 */
	public static void closeJedis(Jedis jedis) {
		pool.returnResource(jedis);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.qhelp.mq.QueueService#push(java.lang.String, java.lang.String)
	 */
	@Override
	public Long push(String key, String value) {
		Jedis jedis = pool.getResource();
		try {
			return jedis.rpush(key, value);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pool.returnResource(jedis);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.qhelp.mq.QueueService#pop(java.lang.String)
	 */
	@Override
	public String pop(String key) {
		Jedis jedis = pool.getResource();
		String result = null;
		try {
			result = jedis.lpop(key);
		} finally {
			pool.returnResource(jedis);
		}

		return result;
	}
}