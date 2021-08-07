package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.RecommendCalculater;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserRecommendService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserMatch;
import com.corgi.user.entity.UserProfile;
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

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.USER_RECOMMEND_QUEUE)
public class UserRecommendConsumer {
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        Long now = System.currentTimeMillis();
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        int size = 1000;
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -30);
        long time = calendar.getTimeInMillis();
        List<String> followUserIds = corgiUserFollowService.getFollowUser(userId);
        followUserIds.add(userId);
        List<String> fanList = new ArrayList<>();
        HashMap<String, Double> weightMap = new HashMap<>();
        HashMap<String, Double> recMap = new HashMap<>();
        List<UserProfile> followUsers = corgiUserFollowService.getFollowUserByPage(userId, "new", 0.0, 0.0, 1, 100);
        log.info("init:" + (System.currentTimeMillis() - now) + "ms ");
        now = System.currentTimeMillis();
        Integer myCount = 0;
        for (UserProfile followUser : followUsers) {
            if (followUser == null) {
                continue;
            }
            int page = 1;
            boolean shouldBreak = false;
            do {
                String key = "followedUser_" + followUser.getUserId() + "-" + page;
                List<String> fanIds = redisTemplate.opsForList().range(key, 0, -1);
                if (!CollectionUtils.isEmpty(fanIds)) {
                    for (String fanId : fanIds) {
                        addWeight(fanId, weightMap, fanList);
                    }
                } else {
                    List<UserProfile> fans = corgiUserFollowService.getFollowedUserByPage(followUser.getUserId(), 0l, page, size);
                    if (CollectionUtils.isEmpty(fans)) {
                        break;
                    }
                    for (UserProfile fan : fans) {
                        if (fan == null || fan.getTime() == null) {
                            continue;
                        }
                        if (userId.equals(fan.getUserId())) {
                            continue;
                        }
                        if (fan.getTime() < time) {
                            shouldBreak = true;
                            break;
                        }
                        redisTemplate.opsForList().leftPush(key, fan.getUserId());
                        addWeight(fan.getUserId(), weightMap, fanList);
                    }
                    redisTemplate.expire(key, 6, TimeUnit.HOURS);
                }
                if (shouldBreak) {
                    break;
                }
                page++;
            } while (true);
            myCount++;
        }
        log.info("fan count:" + (System.currentTimeMillis() - now) + "ms ");
        now = System.currentTimeMillis();
        for (String fanId : fanList) {
            Integer followCount = corgiUserFollowService.countFollow(fanId);
            if (followCount > 0) {
                Double weight = weightMap.get(fanId);
                weightMap.put(fanId, weight / Math.sqrt(followCount.doubleValue()));
            }
        }
        log.info("fan weight:" + (System.currentTimeMillis() - now) + "ms ");
        now = System.currentTimeMillis();
        for (String fanId : fanList) {
            int page = 1;
            do {
                boolean shouldBreak = false;
                String key = "followUser_" + fanId + "-" + page;
                List<String> targetIds = redisTemplate.opsForList().range(key, 0, -1);
                if (!CollectionUtils.isEmpty(targetIds)) {
                    for (String targetId : targetIds) {
                        addRec(targetId, weightMap.get(fanId), recMap);
                    }
                } else {
                    List<UserProfile> targets = corgiUserFollowService.getFollowUserByPage(fanId, "active", 0.0, 0.0, page, 1000);
                    if (CollectionUtils.isEmpty(targets)) {
                        break;
                    }
                    for (UserProfile target : targets) {
                        if (target == null || target.getTime() == null) {
                            continue;
                        }
                        if (target.getTime() < time) {
                            shouldBreak = true;
                            break;
                        }
                        if (followUserIds.contains(target.getUserId())) {
                            continue;
                        }
                        redisTemplate.opsForList().leftPush(key, target.getUserId());
                        addRec(target.getUserId(), weightMap.get(fanId), recMap);
                    }
                    redisTemplate.expire(key, 6, TimeUnit.HOURS);
                }
                if (shouldBreak) {
                    break;
                }
                page++;
            } while (true);
        }
        List<Map.Entry<String, Double>> recList = new ArrayList<>(recMap.entrySet());
        int max = recList.size();
        if (recList.size() > 100) {
            Collections.sort(recList, (Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) -> o2.getValue().compareTo(o1.getValue()));
            max = 100;
        }
        log.info("rec weight:" + (System.currentTimeMillis() - now) + "ms ");
        now = System.currentTimeMillis();

        corgiUserRecommendService.clearRecUser(userId);
        if (myCount == 0) {
            myCount = 1;
        }
        for (int i = 0; i < max; i++) {
            String recId = recList.get(i).getKey();
            Double weight = recList.get(i).getValue();
            log.info("rec:{}:{}:{}:{} ", userId, recId, weight, myCount);
            Double finalWeight = weight / Math.sqrt(myCount.doubleValue());
            //if (finalWeight > 10) {
            corgiUserRecommendService.addRecUser(userId, recId, finalWeight);
            //}
        }
        log.info("add rec:" + (System.currentTimeMillis() - now) + "ms ");
    }

    private void addRec(String recId, Double weight, HashMap<String, Double> recMap) {
        Double tmpWeight = recMap.get(recId);
        if (tmpWeight == null) {
            tmpWeight = weight;
        } else {
            tmpWeight += weight;
        }
        recMap.put(recId, tmpWeight);
    }

    private void addWeight(String userId, HashMap<String, Double> weightMap, List<String> fanList) {
        Double weight = weightMap.get(userId);
        if (weight == null) {
            weight = 1.0;
            fanList.add(userId);
        } else {
            weight++;
        }
        weightMap.put(userId, weight);
    }
}
