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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.USER_GROUP_QUEUE)
public class UserGroupConsumer {
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
        if (!StringUtils.isEmpty(userId)) {
            return;
        }
        UserDetail userDetail = corgiUserService.getUserDetailBasic(userId);
        if (userDetail == null || (!StringUtils.isEmpty(userDetail.getAvatarStatus()) && userDetail.getAvatarStatus().startsWith("fake"))) {
            return;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -180);
        Double totalScore = 0.0;
        Integer fans = 0;
        HashMap<String, Double> groupMap = corgiUserRecommendService.getGroupCor(userId);
        for (String group : groupMap.keySet()) {
            groupMap.put(group, 0.0);
        }
        List<String> addGroup = new ArrayList<>();
        for (int i = 1; i < 100; i++) {
            List<UserProfile> followUsers = corgiUserFollowService.getFollowedUserByPage(userId, 0, i, 10000);
            if (CollectionUtils.isEmpty(followUsers)) {
                break;
            }
            for (UserProfile followUser : followUsers) {
                if (followUser == null) {
                    continue;
                }
                if (!StringUtils.isEmpty(followUser.getAvatarStatus()) && followUser.getAvatarStatus().startsWith("fake")) {
                    continue;
                }
                HashMap<String, Double> preferMap = corgiUserRecommendService.getPreferCor(followUser.getUserId());
                if (CollectionUtils.isEmpty(preferMap)) {
                    continue;
                }
                Double oldTotalScore = totalScore;
                for (String group : preferMap.keySet()) {
                    Double score = groupMap.get(group);
                    if (score == null) {
                        score = 0.0;
                        addGroup.add(group);
                    }
                    totalScore += preferMap.get(group);
                    groupMap.put(group, score + preferMap.get(group));
                }
                if (oldTotalScore < totalScore) {
                    fans++;
                }
            }
        }
        if (totalScore == 0) {
            if (!CollectionUtils.isEmpty(groupMap)) {
                for (String group : groupMap.keySet()) {
                    corgiUserRecommendService.updateGroupCor(userId, group, 0.0);
                }
            }
            return;
        }
        for (String group : addGroup) {
            corgiUserRecommendService.addGroupCor(userId, group);
        }
        long totalFans = corgiStatisticService.sumCount("fans", userId, "");
        for (String group : groupMap.keySet()) {
            corgiUserRecommendService.updateGroupCor(userId, group, groupMap.get(group) * 10000.0 * fans / (totalScore * totalFans));
        }
    }

}
