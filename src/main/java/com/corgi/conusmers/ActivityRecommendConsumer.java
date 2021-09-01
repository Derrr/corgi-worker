package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.RecommendCalculater;
import com.corgi.user.api.CorgiLikeService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserRecommendService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.ActivityLike;
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

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.ACTIVITY_RECOMMEND_QUEUE)
public class ActivityRecommendConsumer {
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiLikeService corgiLikeService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        HashMap<String, Double> weightMap = new HashMap<>();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -30);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String monthAgo = sdf.format(calendar.getTime());
        List<String> likeIds = corgiLikeService.getLikedActivity(userId, null, null, 1, 500);
        Integer myLikeCount = 0;
        int size = 1000;
        for (String activityId : likeIds) {
            int page = 1;
            do {
                boolean shouldBreak = false;
                List<ActivityLike> activityLikes = corgiLikeService.getActivityLike(activityId, page, size);
                if (CollectionUtils.isEmpty(activityLikes)) {
                    break;
                }
                for (ActivityLike like : activityLikes) {
                    String likeUserId = like.getLikeUserId();
                    if (likeUserId.equals(userId) || "fake".equals(like.getType())) {
                        continue;
                    }
                    if (monthAgo.compareTo(like.getCtime()) > 0) {
                        shouldBreak = true;
                        break;
                    }
                    addWeight(likeUserId, weightMap);
                }
                if (shouldBreak) {
                    break;
                }
                page++;
            } while (true);
            myLikeCount++;
        }
        List<Map.Entry<String, Double>> recList = new ArrayList<>(weightMap.entrySet());
        for (Map.Entry<String, Double> entry : recList) {
            String fanId = entry.getKey();
            Integer followCount = corgiLikeService.countUserLikeByDate(fanId, monthAgo);
            if (followCount > 0) {
                entry.setValue(entry.getValue() / Math.sqrt(followCount.doubleValue()));
            }
        }
        int max = recList.size();
        if (recList.size() > 100) {
            Collections.sort(recList, (Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) -> o2.getValue().compareTo(o1.getValue()));
            max = 100;
        }
        corgiUserRecommendService.clearRecActivity(userId);
        if (myLikeCount == 0) {
            myLikeCount = 1;
        }
        for (int i = 0; i < max; i++) {
            String recId = recList.get(i).getKey();
            Double weight = recList.get(i).getValue();
            log.info("adding...{}:{} ", recId, weight);
            Double finalWeight = weight / Math.sqrt(myLikeCount.doubleValue());
            if (finalWeight > 0.2) {
                corgiUserRecommendService.addRecActivity(userId, recId, finalWeight);
            }
        }
    }

    private void addWeight(String userId, HashMap<String, Double> weightMap) {
        Double weight = weightMap.get(userId);
        if (weight == null) {
            weight = 1.0;
        } else {
            weight++;
        }
        weightMap.put(userId, weight);
    }
}
