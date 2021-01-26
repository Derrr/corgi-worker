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

    @Bean
    public Queue userRecommendQueue() {
        return new Queue(CorgiQueueName.USER_RECOMMEND_QUEUE);
    }

    @Bean
    public Queue feedRefreshQueue() {
        return new Queue(CorgiQueueName.FEED_REFRESH);
    }

    @Bean
    public Queue addInfluencerQueue() {
        return new Queue(CorgiQueueName.INFLUENCER_JOIN_QUEUE);
    }

    @Bean
    public Queue onboardQueue() {
        return new Queue(CorgiQueueName.ONBOARD_QUEUE);
    }

    @Bean
    public Queue leftInfluencerQueue() {
        return new Queue(CorgiQueueName.INFLUENCER_LEFT_QUEUE);
    }

    @Bean
    public Queue registerQueue() {
        return new Queue(CorgiQueueName.REGISTER_QUEUE);
    }

    @Bean
    public Queue barActivityQueue() {
        return new Queue(CorgiQueueName.BAR_ACTIVITY_QUEUE);
    }
}
