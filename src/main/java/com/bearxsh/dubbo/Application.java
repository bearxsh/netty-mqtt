package com.bearxsh.dubbo;

import com.bearxsh.dubbo.api.GreetingService;
import com.bearxsh.dubbo.provider.GreetingServiceImpl;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.ServiceConfig;

import java.util.concurrent.CountDownLatch;

public class Application {
    public static void main(String[] args) throws InterruptedException {
        ServiceConfig<GreetingService> service = new ServiceConfig<>();
        service.setApplication(new ApplicationConfig("first-dubbo-provider"));
        service.setRegistry(new RegistryConfig("zookeeper://127.0.0.1:2181"));
        service.setInterface(GreetingService.class);
        service.setRef(new GreetingServiceImpl());
        service.export();
        System.out.println("dubbo service started");
        new CountDownLatch(1).await();
    }
}
