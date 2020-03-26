package thread;

/**
 * @ClassName: demo
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/3/18 5:14 下午
 */

public class demo {
    public static void main(String[] args) {
       /* windows1 w = new windows1();
        Thread t1 = new Thread(w);
        Thread t2 = new Thread(w);
        Thread t3 = new Thread(w);
        t1.start();
        t2.start();
        t3.start();*/

        windows2 t1 = new windows2();
        windows2 t2 = new windows2();
        windows2 t3 = new windows2();
        t1.start();
        t2.start();
        t3.start();

    }


}


class windows2 extends Thread {
    private static int ticket = 100;
    static Object object = new Object();

    @Override
    public void run() {
        while (true) {
            synchronized (windows2.class) {
                if (ticket > 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + "------" + ticket);
                    ticket--;
                } else {
                    break;
                }
            }
        }
    }
}


class windows1 implements Runnable {
    private int ticket = 100;
    Object object = new Object();

    @Override
    public void run() {
        while (true) {
            synchronized (this) {
                if (ticket > 0) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName() + "------" + ticket);

                    ticket--;
                } else {
                    break;
                }
            }
        }
    }
}