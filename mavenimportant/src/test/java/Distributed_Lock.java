import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;
import java.util.List;
import java.util.UUID;

public class Distributed_Lock {

    public static String lock_withTimeout(String lockName, long acquire_timeout, long timeout) {
        Jedis redis = null;
        String key_identifier = null;
        try {
            String identifier = Thread.currentThread().getName() + ":" + UUID.randomUUID().toString(); // Thread
                                                                                                        // just
                                                                                                        // for
                                                                                                        // debug
            String lockKey = "lock:" + lockName;
            int lockExpire = (int) (timeout / 1000);
            long end = System.currentTimeMillis() + acquire_timeout;
            while (System.currentTimeMillis() < end) {
                redis = RedisUtil.getJedis();
                if (redis == null) {
                    System.out.println(Thread.currentThread().getName()
                            + "  warning!!! lock_withTimeout can not get redis conn ,program repeat for get the conn");
                    Thread.sleep(100);
                    continue;
                }
                if (redis.setnx(lockKey, identifier) == 1) {// execute
                                                            // successfully will
                                                            // return "1"
                    redis.expire(lockKey, lockExpire);
                    key_identifier = identifier;
                    System.out.println(Thread.currentThread().getName() + "  获取锁:"+key_identifier);
                    return key_identifier;
                }
                if (redis.ttl(lockKey) == -1) {
                    redis.expire(lockKey, lockExpire);
                }
                long lockKey_ttl = redis.ttl(lockKey);
                try {
                    System.out.println(Thread.currentThread().getName()
                            + "  lock_withTimeout获取锁竞争失败，休息1秒，继续尝试获取,锁ttl剩余：" + lockKey_ttl);

                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (redis != null) {
                    redis.close();
                }
            }
            System.out.println(Thread.currentThread().getName() + "  获取redis连接失败，放弃获取锁");
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + "  获取锁发生异常");
            e.printStackTrace();
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        return key_identifier;
    }

    public static boolean lock_release(String lockName, String identifier) {
        Jedis redis = null;
        String lockKey = "lock:" + lockName;
        boolean retFlag = false;
        String _temp_identifier_from_redis = "";
        try {
            redis = RedisUtil.getJedis();
            while (true) {
                if (redis == null) {
                    System.out.println(Thread.currentThread().getName()
                            + "  warning!!! lock_release can not get redis function,program repeat for get the conn");
                    try {
                        Thread.sleep(10);
                        redis = RedisUtil.getJedis();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    continue;
                }
                redis.watch(lockKey);
                _temp_identifier_from_redis = redis.get(lockKey);
                if (_temp_identifier_from_redis == null || "".equals(_temp_identifier_from_redis)) {
                    System.out.println(Thread.currentThread().getName() + "  锁已过期失效失效");
                } else if (identifier.equals(_temp_identifier_from_redis)) {
                    long del_result = redis.del(lockKey);
                    if (del_result == 1) {
                        System.out.println(Thread.currentThread().getName() + "  完成任务，释放锁");
                        retFlag = true;
                    } else {
                        System.out.println(Thread.currentThread().getName() + "  释放锁失败，锁已提前释放");
                        //continue;
                    }
                } else {
                    System.out.println(Thread.currentThread().getName() + "  锁已过期失效，被污染");
                }
                redis.unwatch();
                break;
            }
        } catch (JedisException e) {
            e.printStackTrace();
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
        return retFlag;
    }
}