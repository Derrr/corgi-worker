package com.corgi;

import com.alibaba.dubbo.spring.boot.annotation.EnableDubboConfiguration;
import com.corgi.common.CorgiQueueName;
import io.netty.buffer.ByteBuf;
import org.springframework.amqp.core.Queue;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableDubboConfiguration
public class CorgiWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(CorgiWorkerApplication.class, args);
    }

    @Bean
    public Queue refreshMatchQueue() {
        return new Queue(CorgiQueueName.REFRESH_MATCH_QUEUE);
    }
}
