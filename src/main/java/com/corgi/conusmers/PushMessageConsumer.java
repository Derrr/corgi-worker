package com.corgi.conusmers;

import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.MatchRefresher;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.PUSH_MESSAGE_QUEUE)
public class PushMessageConsumer {
    @Autowired
    private PushService pushService;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        pushService.sendMessage(pushMessage);
    }
}
