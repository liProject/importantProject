package flinkasync;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * * @param <IN> The type of the input elements.
 * * @param <OUT> The type of the returned elements.
 */
public class AsyncFunDemo extends RichAsyncFunction<String,String> {

    JedisCluster jedisCluster = null;

    /**
     * 初始化redis
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        HashSet<HostAndPort> set = new HashSet<>();
        set.add(new HostAndPort("hadoop4",7001));
        set.add(new HostAndPort("hadoop4",7000));
        set.add(new HostAndPort("hadoop5",7001));
        set.add(new HostAndPort("hadoop5",7000));
        set.add(new HostAndPort("hadoop6",7001));
        set.add(new HostAndPort("hadoop6",7000));

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMinIdle(2);

        /**
         * public JedisCluster(
         *  Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig)
         */
        jedisCluster = new JedisCluster(set, jedisPoolConfig);
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
        //completableFuture发起异步请求
        /**
         * public static <U> CompletableFuture<U> supplyAsync(
         *  Supplier<U> supplier)
         */
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                //解析input获取name列的数据
                String result = "错误";
                String[] arr = input.trim().split(",");
                if (arr.length == 2 && null != arr[1]){
                    String name = arr[1];
                    result = jedisCluster.hget("AsyncReadRedis", name);
                }
                return result;
            }
        }).thenAccept((String str) -> {
            //thenAccept接收异步返回数据，str就是result的值
            //使用结果对象的集合来完成将来的结果。
            resultFuture.complete(
                    //返回仅包含指定对象的不可变集合,返回的集合是可序列化的。
                    Collections.singleton(str));
        });
    }
}
