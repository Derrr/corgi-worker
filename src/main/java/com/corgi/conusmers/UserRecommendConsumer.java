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
        for (UserProfile followUser : followUsers) {
            if (followUser == null) {
                continue;
            }
            int page = 1;
            do {
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
                        continue;
                    }
                    addWeight(fan.getUserId(), weightMap, fanList);
                }
                page++;
            } while (true);
        }

        for (String fanId : fanList) {
            Integer followCount = corgiUserFollowService.countFollow(fanId);
            if (followCount > 0) {
                Double weight = weightMap.get(fanId);
                weightMap.put(fanId, weight / Math.sqrt(followCount.doubleValue()));
            }
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
                    addRec(target.getUserId(), weightMap.get(fanId), recMap);

                }
                page++;
                if (shouldBreak) {
                    break;
                }
            } while (true);
        }
        List<Map.Entry<String, Double>> recList = new ArrayList<>(recMap.entrySet());
        if (recList.size() > 100) {
            Collections.sort(recList, (Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) -> o2.getValue().compareTo(o1.getValue()));
        }
        for (int i = 0; i < 100; i++) {
            String recId = recList.get(i).getKey();
            Double weight = recList.get(i).getValue();
            corgiUserRecommendService.addRecUser(userId, recId, weight);
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
