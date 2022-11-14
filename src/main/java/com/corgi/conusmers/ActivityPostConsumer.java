package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.activity.api.CorgiActivityService;
import com.corgi.activity.entity.CorgiActivity;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.common.messages.RecommendCalculater;
import com.corgi.entity.ActivityQuery;
import com.corgi.user.api.CorgiLikeService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.api.CorgiVlogService;
import com.corgi.user.entity.ActivityLike;
import com.corgi.user.entity.CorgiVlogHot;
import com.corgi.user.entity.UserDetail;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.ACTIVITY_POST_QUEUE)
public class ActivityPostConsumer {
    @Reference
    private CorgiLikeService corgiLikeService;
    @Reference
    private CorgiVlogService corgiVlogService;
    @Reference
    private CorgiActivityService corgiActivityService;
    @Reference
    private CorgiUserService corgiUserService;
    @Autowired
    private StringRedisTemplate redisTemplate;

    private final List<String> WHITE_LIST = Arrays.asList("744758", "521198", "600670", "528454");


    @RabbitHandler
    public void process(CorgiActivity activity) {
        String lockKey = "on_hot-" + activity.getUserId();
        if (redisTemplate.hasKey(lockKey)) {
            return;
        }
        if (CorgiActivity.CAT_TEXT.equals(activity.getCategory())) {
            return;
        }
        UserDetail userDetail = corgiUserService.getUserDetailBasic(activity.getUserId());
        if (userDetail == null) {
            return;
        }
        if (WHITE_LIST.contains(activity.getUserId())) {
            this.onHot(activity, lockKey);
            return;
        }
        if ("influencer".equals(userDetail.getAvatarStatus())) {
            this.onHot(activity, lockKey);
            return;
        }
        String activityId = activity.getId();
        ActivityQuery query = new ActivityQuery();
        query.setPageSize(10);
        query.setUserId(activity.getUserId());
        List<CorgiActivity> corgiActivities = corgiActivityService.getFeedActivity(query);
        if (CollectionUtils.isEmpty(corgiActivities)) {
            double total = 0.0;
            int count = 0;
            int max = 0;
            for (CorgiActivity activity1 : corgiActivities) {
                if (activityId.equals(activity1.getId())) {
                    continue;
                }
                count++;
                Integer likes = corgiLikeService.countRealActivityLike(activity1.getId());
                total += likes;
                if (likes > max) {
                    max = likes;
                }
            }
            if (max >= 50 || total / count > 5) {
                this.onHot(activity, lockKey);
            }
        }
    }

    private void onHot(CorgiActivity activity, String lockKey) {
        if (!redisTemplate.opsForValue().setIfAbsent(lockKey, System.currentTimeMillis() + "", 20L, TimeUnit.HOURS)) {
            return;
        }
        CorgiVlogHot corgiVlogHot = new CorgiVlogHot();
        corgiVlogHot.setViewCount(null);
        corgiVlogHot.setLikeCount(0);
        corgiVlogHot.setActivityId(activity.getId());
        corgiVlogHot.setExpectView(3000);
        corgiVlogHot.setType(CorgiVlogHot.TYPE.MANUAL);
        corgiVlogService.addHotVlog(corgiVlogHot);
        //corgiActivityService.updateByColumn(corgiVlogHot.getActivityId(), "checkStatus", "good");
    }

}
