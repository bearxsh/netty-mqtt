package com.bearxsh.test;

public class Test6 {
    public static void main(String[] args) throws InterruptedException {
        Object o = new Object();
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                synchronized (o) {
                    try {
                        o.wait();
                        System.out.println(Thread.currentThread().getName());
                    } catch (InterruptedException e) {
                        System.out.println(Thread.currentThread().isInterrupted());
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        Thread.sleep(3000);
        synchronized (o) {
            o.notifyAll();
        }

    }
}
