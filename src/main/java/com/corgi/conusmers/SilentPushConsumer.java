package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.*;
import com.corgi.user.entity.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.SILENT_PUSH_QUEUE)
public class SilentPushConsumer {
    public static Double EARTH_RADIUS = 6371.393;

    @Autowired
    private PushService pushService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserService corgiUserService;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        HashMap extra = pushMessage.getExtra();
        if (extra == null) {
            extra = new HashMap();
        }
        extra.put("em_ignore_notification", true);
        pushMessage.setExtra(extra);

        log.info("received silent message: " + pushMessage);
        if (PushMessage.FOLLOW.equals(pushMessage.getType())) {
            sendFollowed(pushMessage);
        } else if (PushMessage.CITY.equals(pushMessage.getType())) {
            String city = (String) pushMessage.getExtra().get("city");
            int page = 1;
            int pageSize = 500;
            while (true) {
                List<UserPosition> userPositions = corgiUserService.getFollowedCityUser(null, city, page, pageSize);
                if (CollectionUtils.isEmpty(userPositions)) {
                    break;
                }
                page++;
                sendBatchPosition(userPositions, pushMessage);
            }
        } else {
            pushService.sendMessage(pushMessage);
        }
    }

    private void sendFollowed(PushMessage pushMessage) {
        List<UserProfile> userProfiles;
        int page = 1;
        int pageSize = 500;
        String sourceId = pushMessage.getSourceUserId();
        while (true) {
            userProfiles = corgiUserFollowService.getFollowedUserByPage(sourceId, 0L, page, pageSize);
            page++;
            sendBatch(userProfiles, pushMessage);
            if (CollectionUtils.isEmpty(userProfiles)) {
                break;
            }
        }
    }

    private Integer sendBatchPosition(List<UserPosition> userPositions, PushMessage pushMessage) {
        int i = 0;
        if (CollectionUtils.isEmpty(userPositions)) {
            return i;
        }
        List<String> registrationIds = new ArrayList<>();
        for (UserPosition userPosition : userPositions) {
            if (userPosition == null) {
                continue;
            }
            registrationIds.add(userPosition.getUserId());
            i++;
        }
        if (CollectionUtils.isNotEmpty(registrationIds)) {
            pushService.sendMessage(pushMessage, registrationIds);
        }
        return i;
    }


    private void sendBatch(List<UserProfile> userProfileList, PushMessage pushMessage) {
        if (CollectionUtils.isEmpty(userProfileList)) {
            return;
        }
        List<String> registrationIds = new ArrayList<>();
        for (UserProfile userProfile : userProfileList) {
            if (userProfile == null) {
                continue;
            }
            registrationIds.add(userProfile.getUserId());
        }
        if (CollectionUtils.isNotEmpty(registrationIds)) {
            pushService.sendMessage(pushMessage, registrationIds);
        }
    }


}
