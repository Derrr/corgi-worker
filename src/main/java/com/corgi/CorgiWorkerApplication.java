package com.corgi;

import com.alibaba.dubbo.spring.boot.annotation.EnableDubboConfiguration;
import com.corgi.common.CorgiQueueName;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * @author tairanliu
 */
@SpringBootApplication
@EnableDubboConfiguration
public class CorgiWorkerApplication {
    @Value("${push.masterSecret}")
    private String MASTER_SECRET;

    @Value("${push.appKey}")
    private String APP_KEY;

    public static void main(String[] args) {
        SpringApplication.run(CorgiWorkerApplication.class, args);
    }

    @Bean
    public Queue refreshMatchQueue() {
        return new Queue(CorgiQueueName.REFRESH_MATCH_QUEUE);
    }

    @Bean
    public Queue traceFollowQueue() {
        return new Queue(CorgiQueueName.TRACE_FOLLOW_QUEUE);
    }

    @Bean
    public Queue pushMessageQueue() {
        return new Queue(CorgiQueueName.PUSH_MESSAGE_QUEUE);
    }

//    @Bean
//    public JPushClient getJPushClient(){
//        JPushClient jpushClient = new JPushClient(MASTER_SECRET, APP_KEY, null, ClientConfig.getInstance());
//        return jpushClient;
//    }
}
