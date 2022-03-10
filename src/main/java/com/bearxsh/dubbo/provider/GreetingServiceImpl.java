package com.bearxsh.dubbo.provider;

import com.bearxsh.dubbo.api.GreetingService;

public class GreetingServiceImpl implements GreetingService {
    @Override
    public String sayHello(String name) {
        return "hello " + name;
    }
}
