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
import org.springframework.beans.BeanUtils;
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
    public static Double EARTH_RADIUS = 6371.393;

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
    @Reference
    private CorgiBlacklistService corgiBlacklistService;
    @Reference
    private CorgiUserDateService corgiUserDateService;
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
            String key = "followUser_" + pushMessage.getSourceUserId() + "_" + pushMessage.getTargetUserId();
            //if (redisTemplate.opsForValue().setIfAbsent(key, "1", 10L, TimeUnit.MINUTES)) {
                pushService.sendMessage(pushMessage);
            //}
        } else if (PushMessage.MATCH.equals(pushMessage.getType())) {
            //this.checkMet(pushMessage);
            List<String> userIds = getUserProfileList(pushMessage);
            String dateId = pushMessage.getSourceUserId();
            HashMap extra = pushMessage.getExtra();
            Double lat = Double.valueOf(extra.get("lat").toString());
            Double lng = Double.valueOf(extra.get("lng").toString());
            if (CollectionUtils.isNotEmpty(userIds)) {
                for (String userId : userIds) {
//                    UserPosition position = corgiUserService.getUserPosition(userId);
//                    Double distance = this.getDistance(lat, lng, position);
//                    if (distance != null && distance < 0.02) {
//                        this.checkDate(dateId, userId);
//                    }
                    String key = "match90sent_" + pushMessage.getSourceUserId() + "_" + userId;
                    if (!redisTemplate.opsForValue().setIfAbsent(key, System.currentTimeMillis() + "", 30L, TimeUnit.DAYS)) {
                        continue;
                    }
                    String userKey = "match90sentUser_" + userId;
                    if (redisTemplate.hasKey(userKey)) {
                        continue;
                    }
                    int match = corgiUserFollowService.isFollowed(pushMessage.getSourceUserId(), userId);
                    if (match >= 3) {
                        if (!redisTemplate.opsForValue().setIfAbsent(userKey, System.currentTimeMillis() + "", 20L, TimeUnit.HOURS)) {
                            continue;
                        }
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
            pushMessage.setSourceUserId(PushService.CORGI_HELPER);
            List<String> beBlackList = corgiBlacklistService.getBeBlacked(userId);
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
                    if (position != null && (userId.equals(position.getUserId()) || beBlackList.contains(position.getUserId()))) {
                        itPosition.remove();
                    }
                }
                page++;
                total += sendBatchPosition(userPositions, pushMessage);
            }
            PushMessage reply = new PushMessage();
            reply.setSourceUserId(PushService.CORGI_HELPER);
            reply.setTargetUserId(userId);
            reply.setMessage("嘿！你的“一呼百应”触发成功，已经告知了活动地点附近 " + total + " 个小哥哥哦，等待一个小红点吧");
            pushService.sendMessage(reply);
        } else if (PushMessage.CITY.concat("_user").equals(pushMessage.getType())) {

            String city = (String) pushMessage.getExtra().get("city");

            String userId = pushMessage.getSourceUserId();
            pushMessage.setSourceUserId(PushService.CORGI_HELPER);
            int page = 1;
            int pageSize = 500;
            while (true) {
                List<UserPosition> userPositions = corgiUserService.getFollowedCityUser(null, city, page, pageSize);
                if (CollectionUtils.isEmpty(userPositions)) {
                    break;
                }
                Iterator<UserPosition> itPosition = userPositions.iterator();
                while (itPosition.hasNext()) {
                    UserPosition position = itPosition.next();
                    int match = corgiUserFollowService.isFollowed(userId, position.getUserId());
                    if (match < 3) {
                        itPosition.remove();
                    } else {
                        String key = "match90sent_" + pushMessage.getSourceUserId() + "_" + position.getUserId();
                        Boolean result = redisTemplate.opsForValue().setIfAbsent(key, "1", 7L, TimeUnit.DAYS);
                        if (!result) {
                            itPosition.remove();
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
        pushMessage.setSourceUserId(PushService.CORGI_HELPER);
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
        pushMessage.setSourceUserId(PushService.CORGI_HELPER);
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
            pushMessage.setSourceUserId(PushService.CORGI_HELPER);
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

//    private void checkDate(String dateId, String userId) {
//        CorgiDateApply apply = corgiUserDateService.getUserApply(dateId, userId);
//        if (apply == null || !"agree".equals(apply.getStatus()) || !"ongoing".equals(apply.getProgress())) {
//            return;
//        }
//        HashMap dateExtra = new HashMap();
//        dateExtra.put("type", "404");
//        dateExtra.put("userId", userId);
//        PushMessage message = new PushMessage();
//        message.setSourceUserId(userId);
//        message.setTargetUserId(dateId);
//        message.setExtra(dateExtra);
//        message.setMessage("你的约会对象进入了你身边20m哦～");
//        pushService.sendMessage(message);
//        message.setSourceUserId(dateId);
//        message.setTargetUserId(userId);
//        pushService.sendMessage(message);
//        apply.setProgress("met");
//        corgiUserDateService.updateApplyProgress(apply);
//    }

//    private void checkMet(PushMessage pushMessage) {
//        HashMap extra = pushMessage.getExtra();
//        Double lat = Double.valueOf(extra.get("lat").toString());
//        if (lat > 90) {
//            return;
//        }
//        Double lng = Double.valueOf(extra.get("lng").toString());
//        if (lng > 200) {
//            return;
//        }
//        String userId = pushMessage.getSourceUserId();
//        List<CorgiDateApply> metApplies = corgiUserDateService.getMetApply(userId);
//        if (metApplies == null) {
//            return;
//        }
//
//        for (CorgiDateApply apply : metApplies) {
//            String dateId = apply.getApplyUserId();
//            if (userId.equals(dateId)) {
//                dateId = apply.getApprovalUserId();
//            }
//            UserPosition position = corgiUserService.getUserPosition(dateId);
//            Double distance = this.getDistance(lat, lng, position);
//            if (distance != null && distance > 3.0) {
//                HashMap dateExtra = new HashMap();
//                dateExtra.put("type", "403");
//                dateExtra.put("userId", userId);
//                PushMessage message = new PushMessage();
//                message.setSourceUserId(userId);
//                message.setTargetUserId(dateId);
//                message.setExtra(dateExtra);
//                message.setMessage("约会已完成，快去对他评价吧～");
//                pushService.sendMessage(message);
//                message.setSourceUserId(dateId);
//                message.setTargetUserId(userId);
//                pushService.sendMessage(message);
//                apply.setProgress("finished");
//                corgiUserDateService.updateApplyProgress(apply);
//            }
//        }
//    }

    private Double getDistance(Double lat, Double lng, UserPosition position) {
        if (position.getLat() > 90 || position.getLng() > 180) {
            return null;
        }
        return Math.acos((Math.sin(lat) * Math.sin(position.getLat())) + (Math.cos(lat) * Math.cos(position.getLat()) * Math.cos(lng - position.getLng()))) * EARTH_RADIUS;
    }

    private List<String> getUserProfileList(PushMessage pushMessage) {
        HashMap extra = pushMessage.getExtra();
        Double lat = Double.valueOf(extra.get("lat").toString());
        if (lat > 90) {
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
