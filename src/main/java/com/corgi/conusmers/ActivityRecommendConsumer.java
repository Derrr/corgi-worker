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
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        HashMap<String, Integer> weightMap = new HashMap<>();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -30);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String monthAgo = sdf.format(calculater.getTime());
        corgiUserRecommendService.clearRecActivity(userId);
        List<String> likeIds = corgiLikeService.getLikedActivity(userId, 1, 500);
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
        }
        List<Map.Entry<String, Integer>> recList = new ArrayList<>(weightMap.entrySet());
        if (recList.size() > 100) {
            Collections.sort(recList, (Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) -> o2.getValue().compareTo(o1.getValue()));
        }
        for (int i = 0; i < 100; i++) {
            String recId = recList.get(i).getKey();
            Integer weight = recList.get(i).getValue();
            corgiUserRecommendService.addRecActivity(userId, recId, weight);
        }
    }

    private void addWeight(String userId, HashMap<String, Integer> weightMap) {
        Integer weight = weightMap.get(userId);
        if (weight == null) {
            weight = 1;
        } else {
            weight++;
        }
        weightMap.put(userId, weight);
    }
}
