package com.bearxsh.test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Test5 {
    public static void main(String[] args) {
        ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(false);
        ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
        ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();
      /*  new Thread(() -> {
            readLock.lock();
            System.out.println(Thread.currentThread().getName());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            readLock.unlock();
        }).start();*/
        new Thread(() -> {
            writeLock.lock();
            System.out.println(Thread.currentThread().getName());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            writeLock.unlock();
        }).start();
        new Thread(() -> {
            writeLock.lock();
            System.out.println(Thread.currentThread().getName());
        }).start();
        new Thread(() -> {
            readLock.lock();
            System.out.println(Thread.currentThread().getName());
        }).start();
    }
}
