package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Redis工具类
 * 
 * @author mxlee
 *
 */
public class RedisUtil {
	protected static Logger logger = LoggerFactory.getLogger(RedisUtil.class);
	public static final String HOST = "127.0.0.1";
	public static final int PORT = 6379;
	public static final Set<HostAndPort> clusterNodes;


	static {
        clusterNodes = new HashSet<HostAndPort>();
        clusterNodes.add(new HostAndPort("192.168.0.203", 6379));
        clusterNodes.add(new HostAndPort("192.168.0.204", 6379));
        clusterNodes.add(new HostAndPort("192.168.0.205", 6379));
        clusterNodes.add(new HostAndPort("192.168.0.206", 6379));
        clusterNodes.add(new HostAndPort("192.168.0.207", 6379));
        clusterNodes.add(new HostAndPort("192.168.0.208", 6379));
    }

	private RedisUtil() {
	}

	private static JedisPool jedisPool = null;
	private static JedisCluster jedisCluster = null;

	/**
	 * 初始化JedisPool
	 * 
	 * @return
	 */
	private static void initialPool() {

		if (jedisPool == null) {
			JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
			// 指定连接池中最大的空闲连接数
			jedisPoolConfig.setMaxIdle(100);
			// 连接池创建的最大连接数
			jedisPoolConfig.setMaxTotal(500);
			// 设置创建连接的超时时间
			jedisPoolConfig.setMaxWaitMillis(1000 * 50);
			// 表示从连接池中获取连接时，先测试连接是否可用
			jedisPoolConfig.setTestOnBorrow(true);
			jedisPool = new JedisPool(jedisPoolConfig, HOST, PORT);

            jedisCluster = new JedisCluster(clusterNodes, 1500, 100, jedisPoolConfig);
        }

	}

	/**
	 * 在多线程环境同步初始化
	 */
	private static synchronized void poolInit() {
		if (jedisPool == null) {
			initialPool();
		}
	}

	/**
	 * 同步获取Jedis实例
	 * 
	 * @return Jedis
	 */
	public synchronized static Jedis getJedis() {
		if (jedisPool == null) {
			poolInit();
		}
		Jedis jedis = null;
		try {
			if (jedisPool != null) {
				jedis = jedisPool.getResource();
			}
		} catch (Exception e) {
			logger.error("获取jedis出错: " + e);
		} finally {
			returnResource(jedis);
		}
        return jedis;
	}


	/**
	 * @Author li
	 * @Description 我自己编写的通过jediscluster获得集群所有jedis操作对象
     *                  jedis简单使用阻塞的I/o和redis交互，Redission是通过netty支持非阻塞I/O,也是一种redis的客户端
     *                  对于redis集群的加锁，redis有一种RedLock（红锁）的算法
	 * @Date 20:14 2019/11/12
	 * @Param []
	 * @return java.util.Set<redis.clients.jedis.Jedis>
	 **/
	public synchronized static Set<Jedis> getJedisForClusetr(){
        HashSet<Jedis> jedisSet = new HashSet<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        Set<Map.Entry<String, JedisPool>> entries = clusterNodes.entrySet();
        for (Map.Entry<String, JedisPool> entry : entries) {
            JedisPool value = entry.getValue();
            jedisSet.add(value.getResource());
        }
        return jedisSet;
    }



	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public static void returnResource(Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			// Jedis3.0之后，returnResource遭弃用，官方重写了close方法
			// jedisPool.returnResource(jedis);
			jedis.close();
		}
	}

	/**
	 * 释放jedis资源
	 * 
	 * @param jedis
	 */
	public static void returnBrokenJedis(Jedis jedis) {
		if (jedis != null && jedisPool != null) {
			jedisPool.returnBrokenResource(jedis);
		}
		jedis = null;
	}

}
