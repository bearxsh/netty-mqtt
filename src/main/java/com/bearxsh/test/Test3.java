package com.bearxsh.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test3 {
    static int a = 0;
    public static void main(String[] args) throws InterruptedException {
        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(10000);
        for (int i = 0; i < 10000; i++) {
            new Thread(() -> {
                a++;
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        System.out.println(a);
        System.out.println(System.currentTimeMillis() - start);
    }
}
