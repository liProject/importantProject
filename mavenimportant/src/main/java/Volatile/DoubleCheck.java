package Volatile;

public class DoubleCheck {
    private volatile static DoubleCheck instance = null;

    private DoubleCheck() {

    }

    public static DoubleCheck getInstance() {
        if(instance==null) {
            synchronized (DoubleCheck.class) {
                if(instance==null)
                    instance = new DoubleCheck();
            }
        }
        return instance;
    }
}
