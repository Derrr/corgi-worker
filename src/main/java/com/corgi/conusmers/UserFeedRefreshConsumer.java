package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiConstants;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.MatchRefresher;
import com.corgi.user.api.CorgiFeedService;
import com.corgi.user.api.CorgiUserMatchService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserMatch;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.FEED_REFRESH)
public class UserFeedRefreshConsumer {
    @Reference
    private CorgiFeedService corgiFeedService;

    @RabbitHandler
    public void process(String userId) {
    }

}
