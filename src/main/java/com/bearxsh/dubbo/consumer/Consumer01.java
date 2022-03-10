package com.bearxsh.dubbo.consumer;

import com.bearxsh.dubbo.api.GreetingService;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfig;
import org.apache.dubbo.config.RegistryConfig;

import java.util.concurrent.CountDownLatch;

public class Consumer01 {

    private static String zookeeperHost = System.getProperty("zookeeper.address", "127.0.0.1");

    public static void main(String[] args) throws InterruptedException {
        ReferenceConfig<GreetingService> reference = new ReferenceConfig<>();
        reference.setApplication(new ApplicationConfig("first-dubbo-consumer"));
        reference.setRegistry(new RegistryConfig("zookeeper://" + zookeeperHost + ":2181"));
        reference.setInterface(GreetingService.class);
        // 设置为异步
        //reference.setAsync(true);
        GreetingService service = reference.get();
        String message = service.sayHello("jack");
/*        System.out.println(service.sayHello("jack", new URL("1", "2", 3)));
        System.out.println(service.sayHello("jack", new URL("1", "2", 3)));
        System.out.println(service.sayHello("jack", new URL("1", "2", 3)));*/
        System.out.println(message);
   /*     CompletableFuture<Object> completableFuture = RpcContext.getContext().getCompletableFuture();
        completableFuture.whenComplete((v, t) -> {
            if (t != null) {
                t.printStackTrace();
            } else {
                System.out.println(v);
            }
        });*/
        new CountDownLatch(1).await();
    }
}
