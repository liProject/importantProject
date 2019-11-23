import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import redis.clients.jedis.Jedis;

public class TestDistributed_Lock {

    public static void main(String[] args) {
        System.out.println( " main start");
        for (int i = 0; i < 100; i++) {
            Test_Thread_lock threadA = new Test_Thread_lock();
            threadA.start();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}

class Test_Thread_lock extends Thread {

    public void run() {
        System.out.println(Thread.currentThread().getName() + " 启动运行");
        String indentifier = Distributed_Lock.lock_withTimeout("resource", 500000, 100000);
        if (indentifier == null) {
            System.out.println(Thread.currentThread().getName() + " 获取锁失败，取消任务");
        }else{
            System.out.println(Thread.currentThread().getName() + "  执行任务");
            Distributed_Lock.lock_release("resource", indentifier);
        }
        System.out.println(Thread.currentThread().getName() + " 完成推出");
    }

}