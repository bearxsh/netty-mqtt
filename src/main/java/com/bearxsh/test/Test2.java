package com.bearxsh.test;

import java.util.concurrent.*;

public class Test2 {
    static int a = 0;
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();

        CountDownLatch countDownLatch = new CountDownLatch(10000);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                int a = 1 / 0;
            }
        });
        ///submit.get();
        System.out.println(System.currentTimeMillis() - start);
    }
}
