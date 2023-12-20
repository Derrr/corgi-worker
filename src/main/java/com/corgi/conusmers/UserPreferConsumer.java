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


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        UserDetail userDetail = corgiUserService.getUserDetailBasic(userId);
        if (userDetail == null || (!StringUtils.isEmpty(userDetail.getAvatarStatus()) && userDetail.getAvatarStatus().startsWith("fake"))) {
            return;
        }
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
        if (totalScore == 0) {
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
        long totalFollow = corgiStatisticService.sumCount("follow", userId, "");
        for (String group : preferMap.keySet()) {
            corgiUserRecommendService.updatePreferCor(userId, group, preferMap.get(group) * 1000.0 * follow * follow / (totalScore * totalFollow));
        }
        //更新对方hide_group
        HashMap<String, Double> groupMap = corgiUserRecommendService.getGroupCor(userId);
        Integer minCount = Integer.MAX_VALUE;
        Double maxWeight = 0.0;

        String hideGroup = "0";
        Integer hideGroupCount = 0;

        String maxHideGroup = "0";
        if (!CollectionUtils.isEmpty(groupMap)) {
            for (String key : groupMap.keySet()) {
                Double thresholdWeight = 0.0;
                Integer thresholdCount = Integer.MAX_VALUE - 1;
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
                Double weight = groupMap.get(key);
                if (thresholdWeight < weight && thresholdCount < minCount) {
                    minCount = thresholdCount;
                    hideGroup = key;
                    hideGroupCount = thresholdCount;
                }
                if (weight > maxWeight) {
                    maxWeight = weight;
                    maxHideGroup = key;
                }
            }
            if ("0".equals(hideGroup)) {
                hideGroup = maxHideGroup;
            }
            Long result = redisTemplate.opsForValue().increment("group_weight_" + hideGroup);
            UserDetail update = new UserDetail();
            update.setUserId(userId);
            update.setUptime("1");
            if (result <= hideGroupCount + 1) {
                update.setHideGroup(hideGroup);
            } else {
                update.setHideGroup(maxHideGroup);
            }
            corgiUserService.updateDetail(update);
        }
    }
}
