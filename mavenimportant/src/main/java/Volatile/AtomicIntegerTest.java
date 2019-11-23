package Volatile;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicIntegerTest {
    public  AtomicInteger inc = new AtomicInteger();

    public  void increase() {
        inc.getAndIncrement();
    }

    public static void main(String[] args) {
        final AtomicIntegerTest AtomicIntegerTest = new AtomicIntegerTest();
        for(int i=0;i<10;i++){
            new Thread(){
                public void run() {
                    for(int j=0;j<1000;j++)
                        AtomicIntegerTest.increase();
                };
            }.start();
        }

        while(Thread.activeCount()>1)  //保证前面的线程都执行完
            Thread.yield();
        System.out.println(AtomicIntegerTest.inc);
    }
}
