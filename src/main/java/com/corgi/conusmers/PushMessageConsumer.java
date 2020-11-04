package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
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
    @Reference
    private CorgiPushLogService corgiPushLogService;
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
            }
            pushService.sendMessage(pushMessage);
        } else if (PushMessage.MATCH.equals(pushMessage.getType())) {
            List<String> userIds = getUserProfileList(pushMessage);
            if (CollectionUtils.isNotEmpty(userIds)) {
                for (String userId : userIds) {
                    String key = "match90sent_" + pushMessage.getSourceUserId() + "_" + userId;
                    Boolean result = redisTemplate.opsForValue().setIfAbsent(key, "1", 7L, TimeUnit.DAYS);
                    if (!result) {
                        continue;
                    }
                    int match = corgiUserFollowService.isFollowed(pushMessage.getSourceUserId(), userId);
                    if (match >= 3) {
                        pushMessage.setTargetUserId(userId);
                        pushService.sendMessage(pushMessage);
                        PushLog pushLog = new PushLog();
                        pushLog.setFrom(pushMessage.getSourceUserId());
                        pushLog.setTo(userId);
                        corgiPushLogService.addPushLog(pushLog);
                    }
                }
            }
        } else if (PushMessage.ACTIVITY.equals(pushMessage.getType())) {
            sendFollowed(pushMessage);
        } else if (PushMessage.ACTIVITY.concat("_city").equals(pushMessage.getType())) {
            String city = (String) pushMessage.getExtra().get("city");
            sendFollowedCity(pushMessage, city);
        } else if (PushMessage.CITY.equals(pushMessage.getType())) {
            String city = (String) pushMessage.getExtra().get("city");
            String userId = pushMessage.getSourceUserId();
            pushMessage.setSourceUserId(PushService.HELPER);
            int page = 1;
            int pageSize = 500;
            int total = 0;
            while (true) {
                List<UserPosition> userPositions = corgiUserService.getFollowedCityUser(null, city, page, pageSize);
                if (CollectionUtils.isEmpty(userPositions)) {
                    break;
                }
                Iterator<UserPosition> itPosition = userPositions.iterator();
                while (itPosition.hasNext()) {
                    UserPosition position = itPosition.next();
                    if (position != null && userId.equals(position.getUserId())) {
                        itPosition.remove();
                    }
                }
                page++;
                total += sendBatchPosition(userPositions, pushMessage);
                if (userPositions.size() < pageSize) {
                    break;
                }
            }
            PushMessage reply = new PushMessage();
            reply.setSourceUserId(PushService.HELPER);
            reply.setTargetUserId(userId);
            reply.setMessage("嘿！你的“一呼百应”触发成功，已经告知了活动地点附近 " + total + " 个小哥哥哦，等待一个小红点吧");
            pushService.sendMessage(reply);
        } else if (PushMessage.CITY.concat("_user").equals(pushMessage.getType())) {

            String city = (String) pushMessage.getExtra().get("city");

            String userId = pushMessage.getSourceUserId();
            log.info("city_user .... into" + city + userId);
            pushMessage.setSourceUserId(PushService.HELPER);
            int page = 1;
            int pageSize = 500;
            while (true) {
                List<UserPosition> userPositions = corgiUserService.getFollowedCityUser(null, city, page, pageSize);
                log.info("city_user .... size:" + userPositions.size());
                if (CollectionUtils.isEmpty(userPositions)) {
                    break;
                }
                Iterator<UserPosition> itPosition = userPositions.iterator();
                while (itPosition.hasNext()) {
                    UserPosition position = itPosition.next();
                    log.info("city_user .... userId:" + position.getUserId());
                    if (position != null && ((userId.equals(position.getUserId())
                            || (!position.getVersion().equals("1.5.8") && !position.getVersion().equals("android1.5.4"))))) {
                        itPosition.remove();
                    } else {
                        int match = corgiUserFollowService.isFollowed(pushMessage.getSourceUserId(), position.getUserId());
                        log.info("city_user .... match:" + match);
                        if (match < 3) {
                            itPosition.remove();
                        } else {
                            String key = "match90sent_" + pushMessage.getSourceUserId() + "_" + position.getUserId();
                            //Boolean result = redisTemplate.opsForValue().setIfAbsent(key, "1", 7L, TimeUnit.DAYS);
                            //if (!result) {
                            //itPosition.remove();
                            //}
                        }
                    }
                }
                page++;
                sendBatchPosition(userPositions, pushMessage);
            }
        } else if (PushMessage.ACTIVITY.concat("_end").equals(pushMessage.getType())) {
            sendSignUp(pushMessage);
        } else {
            pushService.sendMessage(pushMessage);
        }
    }

    private void sendFollowedCity(PushMessage pushMessage, String city) {
        int page = 1;
        int pageSize = 500;
        String sourceId = pushMessage.getSourceUserId();
        pushMessage.setSourceUserId(PushService.HELPER);
        while (true) {
            List<UserPosition> userPositions = corgiUserService.getFollowedCityUser(sourceId, city, page, pageSize);
            page++;
            sendBatchPosition(userPositions, pushMessage);
            if (CollectionUtils.isEmpty(userPositions) || userPositions.size() < pageSize) {
                break;
            }
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
            if (CollectionUtils.isEmpty(userProfiles) || userProfiles.size() < pageSize) {
                break;
            }
        }
    }

    private void sendSignUp(PushMessage pushMessage) {
        String activityId = (String) pushMessage.getExtra().get("activityId");
        List<UserProfile> userProfiles = corgiUserActivityService.getUsers(activityId, null, UserSignUp.AGREE + "");
        pushMessage.setSourceUserId(PushService.HELPER);
        sendBatch(userProfiles, pushMessage);
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
            pushMessage.setSourceUserId(PushService.HELPER);
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

    private List<String> getUserProfileList(PushMessage pushMessage) {
        HashMap extra = pushMessage.getExtra();
        Double lat = Double.valueOf(extra.get("lat").toString());
        if (lat > 200) {
            return new ArrayList<>();
        }
        Double lng = Double.valueOf(extra.get("lng").toString());
        if (lng > 200) {
            return new ArrayList<>();
        }
        UserQuery userQuery = new UserQuery();
        userQuery.setLat(lat);
        userQuery.setLng(lng);
        userQuery.setUserId(pushMessage.getSourceUserId());
        userQuery.setRange(3.0);
        return corgiUserService.getAllNearByUser(userQuery);
    }
}
