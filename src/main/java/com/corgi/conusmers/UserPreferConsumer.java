package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.RecommendCalculater;
import com.corgi.user.api.CorgiStatisticService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserRecommendService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserDetail;
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
@RabbitListener(queues = CorgiQueueName.USER_PREFER_QUEUE)
public class UserPreferConsumer {
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;
    @Reference
    private CorgiStatisticService corgiStatisticService;
    @Autowired
    private StringRedisTemplate redisTemplate;

    private static List<String> groupOrder = Arrays.asList("偏瘦", "偏胖", "肌肉", "肉壮", "精壮", "匀称");


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        Long totalFollow = corgiStatisticService.sumCount("follow", userId, "");
        if (totalFollow == 0) {
            return;
        }
        this.updateHideGroup(userId);
        Double totalScore = 0.0;
        Integer follow = 0;
        HashMap<String, Double> preferMap = corgiUserRecommendService.getPreferCor(userId);
        for (String group : preferMap.keySet()) {
            preferMap.put(group, 0.0);
        }
        List<String> addGroup = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            List<UserProfile> followUsers = corgiUserFollowService.getFollowUserByPage(userId, "new", 0.0, 0.0, i, 10000);
            if (CollectionUtils.isEmpty(followUsers)) {
                break;
            }
            for (UserProfile followUser : followUsers) {
                if (followUser == null) {
                    continue;
                }
                HashMap<String, Double> groupMap = corgiUserRecommendService.getGroupCor(followUser.getUserId());
                if (CollectionUtils.isEmpty(groupMap)) {
                    continue;
                }
                Double oldTotalScore = totalScore;
                for (String group : groupMap.keySet()) {
                    Double score = preferMap.get(group);
                    if (score == null) {
                        score = 0.0;
                        addGroup.add(group);
                    }
                    Double groupScore = groupMap.get(group);
                    totalScore += groupScore;
                    preferMap.put(group, score + groupScore);
                }
                if (oldTotalScore < totalScore) {
                    follow++;
                }
            }
        }
        if (totalScore < 1 || follow == 0) {
            if (!CollectionUtils.isEmpty(preferMap)) {
                for (String group : preferMap.keySet()) {
                    corgiUserRecommendService.updatePreferCor(userId, group, 0.0);
                }
            }
            return;
        }
        for (String group : addGroup) {
            corgiUserRecommendService.addPreferCor(userId, group);
        }

        for (String group : preferMap.keySet()) {
            corgiUserRecommendService.updatePreferCor(userId, group, preferMap.get(group) * 10000.0 * follow / (totalFollow * totalScore));
        }
    }

    private void updateHideGroup(String userId) {
        //更新对方hide_group
        HashMap<String, Double> groupMap = corgiUserRecommendService.getGroupCor(userId);
        Integer maxCount = 0;
        String hideGroup = "0";
        UserDetail update = new UserDetail();
        update.setUserId(userId);
        update.setUptime("1");

        if (!CollectionUtils.isEmpty(groupMap)) {
            Double sum = groupMap.values().stream().reduce((m, n) -> m + n).get();
            if (sum > 0.0) {
                List<String> residentGroups = new ArrayList<>();

                for (String key : groupOrder) {
                    Double thresholdWeight = 0.0;
                    Integer thresholdCount = 0;
                    Object weightCache = redisTemplate.opsForHash().get("group_weight", key);
                    Object countCache = redisTemplate.opsForHash().get("group_count", key);
                    try {
                        if (weightCache != null && Double.valueOf(weightCache.toString()) > 0) {
                            thresholdWeight = Double.valueOf(weightCache.toString());
                        }
                        if (countCache != null && Integer.valueOf(countCache.toString()) > 0) {
                            thresholdCount = Integer.valueOf(countCache.toString());
                        }
                    } catch (Exception e) {
                        log.info("group weight goes wrong", e);
                    }
                    Double weight = groupMap.get(key) / sum;
                    if (thresholdWeight < weight) {
                        residentGroups.add(key);
                        if ("0".equals(hideGroup)) {
                            maxCount = thresholdCount;
                            hideGroup = key;
                        }
                    }
                }
                if (!"0".equals(hideGroup)) {
                    Long result = redisTemplate.opsForValue().increment("group_weight_" + hideGroup);
                    if (result <= maxCount) {
                        update.setHideGroup(hideGroup);
                        corgiUserService.updateDetail(update);
                        return;
                    }
                }
                if (residentGroups.size() > 1) {
                    for (String group : groupOrder) {
                        if (!hideGroup.equals(group) && residentGroups.contains(group)) {
                            Long result = redisTemplate.opsForValue().increment("group_weight_" + group);
                            Integer count = Integer.valueOf(redisTemplate.opsForHash().get("group_count", group) + "");
                            if (result <= count) {
                                update.setHideGroup(group);
                                corgiUserService.updateDetail(update);
                                return;
                            }
                        }
                    }
                }
                update.setHideGroup(hideGroup);
                corgiUserService.updateDetail(update);
                return;
            }
        }
        update.setHideGroup("0");
        corgiUserService.updateDetail(update);
    }
}
