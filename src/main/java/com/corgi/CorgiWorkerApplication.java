package com.corgi;

import com.alibaba.dubbo.spring.boot.annotation.EnableDubboConfiguration;
import com.corgi.common.CorgiQueueName;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;

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
    public Queue activityRecommendQueue() {
        return new Queue(CorgiQueueName.ACTIVITY_RECOMMEND_QUEUE);
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

    @Bean
    public Queue userDateQueue() {
        return new Queue(CorgiQueueName.USER_DATE_QUEUE);
    }

    @Bean
    public Queue silentPushQueue() {
        return new Queue(CorgiQueueName.SILENT_PUSH_QUEUE);
    }

    @Bean("pushMessageFactory")
    public SimpleRabbitListenerContainerFactory pushMessageFactory(SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setPrefetchCount(1);
        factory.setConcurrentConsumers(2);
        configurer.configure(factory, connectionFactory);
        return factory;
    }


    @Bean
    public RedisTemplate redisTemplate(RedisConnectionFactory factory) {
        StringRedisTemplate redisTemplate = new StringRedisTemplate(factory);
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        //设置完这个可以直接将对象以json格式存入redis中，但是取出来的时候要用JSON.parseArray(Json.toJsonString(object),Object.class)解析一下
        redisTemplate.setValueSerializer(jackson2JsonRedisSerializer);
        redisTemplate.setHashValueSerializer(jackson2JsonRedisSerializer);
        //调用后完成设置
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
}
