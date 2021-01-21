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
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        corgiUserRecommendService.clearRecUser(userId);
        int countMatch = 0;
        int size = 100;
        int page1 = 1;
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -30);
        long time = calendar.getTimeInMillis();
        List<String> followUserIds = corgiUserFollowService.getFollowUser(userId);
        List<String> fanList = new ArrayList<>();
        HashMap<String, Integer> weightMap = new HashMap<>();
        List<UserProfile> followUsers = corgiUserFollowService.getFollowUserByPage(userId, "new", 0.0, 0.0, 1, 100);
        for (UserProfile followUser : followUsers) {
            int page = 1;
            do {
                boolean shouldBreak = false;
                List<UserProfile> fans = corgiUserFollowService.getFollowedUserByPage(followUser.getUserId(), 0l, page, size);
                if (CollectionUtils.isEmpty(fans)) {
                    break;
                }
                for (UserProfile fan : fans) {
                    if (fan.getTime() < time) {
                        shouldBreak = true;
                        break;
                    }
                    addWeight(fan.getUserId(), weightMap, fanList);
                }
                page++;
                if (shouldBreak) {
                    break;
                }
            } while (true);
        }

        for (String fanId : fanList) {
            int page = 1;
            do {
                boolean shouldBreak = false;
                List<UserProfile> targets = corgiUserFollowService.getFollowUserByPage(fanId, "active", 0.0, 0.0, page, 100);
                if (CollectionUtils.isEmpty(targets)) {
                    break;
                }
                for (UserProfile target : targets) {
                    if (target.getTime() < time) {
                        shouldBreak = true;
                        break;
                    }
                    if (followUserIds.contains(target.getUserId())) {
                        continue;
                    }
                    corgiUserRecommendService.addRecUser(userId, target.getUserId(), weightMap.get(fanId));
                }
                page++;
                if (shouldBreak) {
                    break;
                }
            } while (true);
        }
//        do {
//            List<UserProfile> userProfiles = corgiUserFollowService.getMatchUserByPage(userId, "new", 0.0, 0.0, page1, size);
//            page1++;
//            if (CollectionUtils.isEmpty(userProfiles)) {
//                break;
//            }
//            for (UserProfile userProfile : userProfiles) {
//                if (userProfile == null || userProfile.getUserId() == null) {
//                    continue;
//                }
//                countMatch++;
//                String matchId = userProfile.getUserId();
//                int page2 = 1;
//                do {
//                    List<UserProfile> matchProfiles = corgiUserFollowService.getMatchUserByPage(matchId, "new", 0.0, 0.0, page2, size);
//                    page2++;
//                    if (CollectionUtils.isEmpty(matchProfiles)) {
//                        break;
//                    }
//                    for (UserProfile matchProfile : matchProfiles) {
//                        String resultId = matchProfile.getUserId();
//                        corgiUserRecommendService.addRecUser(userId, resultId);
//
//                    }
//                } while (true);
//            }
//        } while (true);

//        int weight = 1;
//        if (countMatch > 10) {
//            weight = countMatch / 10;
//        }
//        corgiUserRecommendService.deleteRecUserByWeight(userId, weight);
    }

    private void addWeight(String userId, HashMap<String, Integer> weightMap, List<String> fanList) {
        Integer weight = weightMap.get(userId);
        if (weight == null) {
            weight = 1;
            fanList.add(userId);
        } else {
            weight++;
        }
        weightMap.put(userId, weight);
    }
}
