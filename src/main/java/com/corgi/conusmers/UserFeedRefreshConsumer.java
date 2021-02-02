package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiConstants;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.MatchRefresher;
import com.corgi.user.api.*;
import com.corgi.user.entity.*;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.FEED_REFRESH)
public class UserFeedRefreshConsumer {
    @Reference
    private CorgiFeedService corgiFeedService;
    @Reference
    private CorgiVlogService corgiVlogService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;

    @RabbitHandler
    public void process(String userId) {
        if (corgiFeedService.countUnviewFeed(userId) >= 20) {
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String ctime = sdf.format(new Date());
        List<CorgiVlog> result = new ArrayList<>();
        merge(result, recallNewVlog(userId, ctime));
        merge(result, recallRecommendUser(userId, ctime));
        merge(result, recallRecommendVlog(userId, ctime));
        for (CorgiVlog vlog : result) {
            corgiFeedService.addFeed(buildFeed(vlog, userId));
        }
        if (corgiFeedService.countUnviewFeed(userId) < 5) {

        }
    }

    private CorgiFeed buildFeed(CorgiVlog vlog, String userId) {
        CorgiFeed feed = new CorgiFeed();
        feed.setFeed(vlog.getActivityId());
        feed.setUserId(userId);
        feed.setFeedUserId(vlog.getUserId());
        return feed;
    }

    private List<CorgiVlog> recallRecommendVlog(String userId, String ctime) {
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        recall.setType("like");
        return corgiVlogService.recallVlog(recall, 5);
    }

    private List<CorgiVlog> recallRecommendUser(String userId, String ctime) {
        List<UserProfile> userProfiles = corgiUserRecommendService.getRecUser(userId, 100);
        List<CorgiVlog> vlogs = new ArrayList<>();
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        for (UserProfile userProfile : userProfiles) {
            vlogs.addAll(corgiVlogService.recallTargetVlog(userProfile.getUserId(), recall, 1));
            if (vlogs.size() >= 5) {
                return vlogs;
            }
        }
        return vlogs;
    }

    private List<CorgiVlog> recallNewVlog(String userId, String ctime) {
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        return corgiVlogService.recallVlog(recall, 2);
    }

    private List<CorgiVlog> merge(List<CorgiVlog> result, List<CorgiVlog> newVlog) {
        if (CollectionUtils.isEmpty(newVlog)) {
            return result;
        }
        Random random = new Random();
        for (CorgiVlog vlog : newVlog) {
            int index = random.nextInt(result.size() + 1);
            result.add(index, vlog);
        }
        return result;
    }
}
