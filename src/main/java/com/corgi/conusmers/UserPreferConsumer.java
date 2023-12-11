package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.RecommendCalculater;
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
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -90);
        List<UserProfile> followUsers = corgiUserFollowService.getFollowUserByPage(userId, "active", 0.0, 0.0, 1, 10000);
        Integer myCount = 0;
        Double totalScore = 0.0;
        HashMap<String, Double> preferMap = corgiUserRecommendService.getPreferCor(userId);
        for (String group : preferMap.keySet()) {
            preferMap.put(group, 0.0);
        }
        List<String> addGroup = new ArrayList<>();
        for (UserProfile followUser : followUsers) {
            if (followUser == null) {
                continue;
            }
            HashMap<String, Double> groupMap = corgiUserRecommendService.getGroupCor(followUser.getUserId());
            //UserDetail followDetail = corgiUserService.getUserDetailBasic(followUser.getUserId());
//            if (followDetail == null) {
//                continue;
//            }
//            String group = followDetail.getGroup();
            if (CollectionUtils.isEmpty(groupMap)) {
                continue;
            }
            for (String group : groupMap.keySet()) {
                Double score = preferMap.get(group);
                if (score == null) {
                    score = 0.0;
                    addGroup.add(group);
                }
                Double groupScore = groupMap.get(group) * groupMap.get(group);
                totalScore += groupScore;
                preferMap.put(group, score + groupScore);
            }
            myCount++;
        }
        if (myCount < 10 || totalScore == 0) {
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
            corgiUserRecommendService.updatePreferCor(userId, group, Math.sqrt(preferMap.get(group)));
        }
    }

}
