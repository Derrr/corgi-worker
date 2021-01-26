package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.CorgiBlacklistService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserProfile;
import com.corgi.user.entity.UserQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.INFLUENCER_LEFT_QUEUE)
public class PushInfluencerLeftConsumer {

    @Autowired
    private PushService pushService;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        pushMessage.setSourceUserId(PushService.HELPER);
        pushMessage.setMessage("很遗憾，由于长时间未登陆，可小基暂时取消了您的天菜创始人标志，请多多发些动态以及和粉丝们互动哦。");
        pushService.sendMessage(pushMessage);
    }


}
