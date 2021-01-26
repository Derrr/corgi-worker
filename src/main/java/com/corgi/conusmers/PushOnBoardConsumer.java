package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.*;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserProfile;
import com.corgi.user.entity.UserQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.ONBOARD_QUEUE)
public class PushOnBoardConsumer {

    @Autowired
    private PushService pushService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        String userId = pushMessage.getTargetUserId();
        pushMessage.setSourceUserId(PushService.HELPER);
        pushMessage.setMessage("撒花撒花～宝贝你今天被可小基推荐上榜单啦，快发个动态迎接粉丝小哥哥们的崇拜吧。");
        pushService.sendMessage(pushMessage);

        List<String> matchUserIds = new ArrayList<>();
        pushMessage.setMessage("您关注的天菜里，今日又有美人被推荐上Corgi榜单啦，快给他点个赞沾沾喜气，顺便问问他是如何做到上榜的。");
        int page = 1;
        while (true) {
            List<UserProfile> fans = corgiUserFollowService.getFollowedUserByPage(userId, 0l, page, 500);
            if (CollectionUtils.isEmpty(fans)) {
                break;
            }
            List<String> userIds = new ArrayList<>();
            for (UserProfile fan : fans) {
                if (corgiUserFollowService.isFollowed(userId, fan.getUserId()) == 3) {
                    matchUserIds.add(fan.getUserId());
                } else {
                    userIds.add(fan.getUserId());
                }
            }
            pushService.sendMessage(pushMessage, userIds);
            page++;
        }

        pushMessage.setMessage("您的好友里，今日又有美人被推荐上Corgi榜单啦，快给他点个赞沾沾喜气，顺便问问他是如何做到上榜的。");
        pushService.sendMessage(pushMessage, matchUserIds);
    }


}
