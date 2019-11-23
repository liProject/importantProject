package Volatile;

public class SynchronizedTest {
    public  int inc = 0;

    public synchronized void increase() {
        inc++;
    }

    public static void main(String[] args) {
        final SynchronizedTest SynchronizedTest = new SynchronizedTest();
        for(int i=0;i<10;i++){
            new Thread(){
                public void run() {
                    for(int j=0;j<1000;j++)
                        SynchronizedTest.increase();
                };
            }.start();
        }

        while(Thread.activeCount()>1)  //保证前面的线程都执行完
            Thread.yield();
        System.out.println(SynchronizedTest.inc);
    }
}
