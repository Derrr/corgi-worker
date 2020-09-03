package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.CorgiUserActivityService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserMatchService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserPosition;
import com.corgi.user.entity.UserProfile;
import com.corgi.user.entity.UserQuery;
import com.corgi.user.entity.UserSignUp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.PUSH_MESSAGE_QUEUE)
public class PushMessageConsumer {

    @Autowired
    private PushService pushService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserActivityService corgiUserActivityService;
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiUserMatchService corgiUserMatchService;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        log.info("received message: " + pushMessage);
        if (PushMessage.FOLLOW.equals(pushMessage.getType())) {
            int count = corgiUserFollowService.isFollowed(pushMessage.getSourceUserId(), pushMessage.getTargetUserId());
            HashMap extra = pushMessage.getExtra();
            extra.put("userId", pushMessage.getSourceUserId());
            if (count <= 2) {
                extra.put("type", PushMessage.FOLLOW_MESSAGE_TYPE + "");
                pushMessage.setMessage(PushMessage.FOLLOW_MESSAGE);
            } else {
                extra.put("type", PushMessage.MATCH_MESSAGE_TYPE + "");
                pushMessage.setMessage(PushMessage.MATCH_MESSAGE);
                pushService.sendMessage(pushMessage);

                //调换发送者和接受者
                String sourceId = pushMessage.getTargetUserId();
                pushMessage.setTargetUserId(pushMessage.getSourceUserId());
                pushMessage.setSourceUserId(sourceId);
                extra.put("userId", sourceId);
            }
            pushService.sendMessage(pushMessage);
        } else if (PushMessage.MATCH.equals(pushMessage.getType())) {
            List<String> userIds = getUserProfileList(pushMessage);
            if (CollectionUtils.isNotEmpty(userIds)) {
                for (String userId : userIds) {
                    String key = "match90sent_" + pushMessage.getSourceUserId() + "_" + userId;
                    String result = redisTemplate.opsForValue().get(key);
                    if (StringUtils.isNotEmpty(result)) {
                        continue;
                    }
                    int match = corgiUserFollowService.isFollowed(pushMessage.getSourceUserId(), userId);
                    //Double match = corgiUserMatchService.getUserMatch(userId, pushMessage.getSourceUserId());
                    if (match >= 3) {
                        pushMessage.setTargetUserId(userId);
                        pushService.sendMessage(pushMessage);
                        redisTemplate.opsForValue().set(key, System.currentTimeMillis() + "", 1L, TimeUnit.DAYS);
                    }
                }
            }
        } else if (PushMessage.ACTIVITY.equals(pushMessage.getType())) {
            sendFollowed(pushMessage);
        } else if (PushMessage.CITY.equals(pushMessage.getType())) {
            String city = (String) pushMessage.getExtra().get("city");
            int page = 1;
            int pageSize = 500;
            while (true) {
                List<UserPosition> userPositions = corgiUserService.getFollowedCityUser(pushMessage.getSourceUserId(), city, page, pageSize);
                page++;
                sendBatchPosition(userPositions, pushMessage);
                if (CollectionUtils.isEmpty(userPositions) || userPositions.size() < pageSize) {
                    break;
                }
            }
        } else {
            pushService.sendMessage(pushMessage);
        }

    }

    private void sendFollowed(PushMessage pushMessage) {
        List<UserProfile> userProfiles;
        int page = 1;
        int pageSize = 500;
        while (true) {
            userProfiles = corgiUserFollowService.getFollowedUserByPage(pushMessage.getSourceUserId(), 0L, page, pageSize);
            page++;
            sendBatch(userProfiles, pushMessage);
            if (CollectionUtils.isEmpty(userProfiles) || userProfiles.size() < pageSize) {
                break;
            }
        }
    }

    private void sendBatchPosition(List<UserPosition> userPositions, PushMessage pushMessage) {
        if (CollectionUtils.isEmpty(userPositions)) {
            return;
        }
        List<String> registrationIds = new ArrayList<>();
        for (UserPosition userPosition : userPositions) {
            if (userPosition == null) {
                continue;
            }
            registrationIds.add(userPosition.getUserId());
        }
        if (CollectionUtils.isNotEmpty(registrationIds)) {
            pushService.sendMessage(pushMessage, registrationIds);
        }
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

    private List<String> getUserProfileList(PushMessage pushMessage) {
        HashMap extra = pushMessage.getExtra();
        Double lat = Double.valueOf(extra.get("lat").toString());
        Double lng = Double.valueOf(extra.get("lng").toString());
        UserQuery userQuery = new UserQuery();
        userQuery.setLat(lat);
        userQuery.setLng(lng);
        userQuery.setUserId(pushMessage.getSourceUserId());
        userQuery.setRange(3.0);
        return corgiUserService.getAllNearByUser(userQuery);
    }
}
