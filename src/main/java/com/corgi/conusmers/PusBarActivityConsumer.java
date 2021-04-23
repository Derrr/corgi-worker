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
@RabbitListener(queues = CorgiQueueName.BAR_ACTIVITY_QUEUE)
public class PusBarActivityConsumer {

    @Autowired
    private PushService pushService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiBlacklistService corgiBlacklistService;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        String barId = pushMessage.getTargetUserId();
        pushMessage.setSourceUserId(PushService.CORGI_HELPER);

        pushMessage.setMessage("您关注的商户发布了新活动，快约上集美们一起去玩玩呀。");
        int page = 1;
        while (true) {
            List<UserProfile> fans = corgiUserFollowService.getFollowedUserByPage(barId, 0l, page, 500);
            if (CollectionUtils.isEmpty(fans)) {
                break;
            }
            List<String> userIds = new ArrayList<>();
            for (UserProfile fan : fans) {
                userIds.add(fan.getUserId());
            }
            pushService.sendMessage(pushMessage, userIds);
            page++;
        }

    }


}
