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
import java.util.*;
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
    private CorgiBlacklistService corgiBlacklistService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;

    @RabbitHandler
    public void process(String userId) {
        log.info("start feeding...{} ", userId);
        if (corgiFeedService.countUnviewFeed(userId) >= 10) {
            return;
        }
        List<String> blackUserIds = new ArrayList<>();
        List<UserBasic> basicList = corgiBlacklistService.getBlackUser(userId);
        if (!CollectionUtils.isEmpty(basicList)) {
            for (UserBasic basic : basicList) {
                blackUserIds.add(basic.getUserId());
            }
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String ctime = sdf.format(new Date());
        List<CorgiVlog> result = new ArrayList<>();
        result = merge(result, recallNewVlog(userId, ctime), blackUserIds);
        result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.AUTO, 2, "asc"), blackUserIds);
        result = merge(result, recallRecommendUser(userId, ctime), blackUserIds);
        //result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.MANUAL, 1, "asc"), blackUserIds);
        result = merge(result, recallRecommendVlog(userId, ctime, 10 - result.size(), "like"), blackUserIds);
        if (result.size() < 10) {
            result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.AUTO, 10 - result.size(), "desc"), blackUserIds);
        }
        for (CorgiVlog vlog : result) {
            corgiFeedService.addFeed(buildFeed(vlog, userId));
        }
    }

    private List<CorgiVlog> recallHotVlog(String userId, String ctime, String type, Integer size, String orderby) {
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        recall.setType(type);
        recall.setStatus(orderby);
        List<CorgiVlog> vlogs = corgiVlogService.recallHotVlog(recall, size);
        for (CorgiVlog vlog : vlogs) {
            vlog.setType("hot" + type + orderby + "|");
        }
        return vlogs;
    }

    private CorgiFeed buildFeed(CorgiVlog vlog, String userId) {
        CorgiFeed feed = new CorgiFeed();
        feed.setFeed(vlog.getActivityId());
        feed.setUserId(userId);
        feed.setFeedUserId(vlog.getUserId());
        feed.setSource(vlog.getType());
        return feed;
    }

    private List<CorgiVlog> recallRecommendVlog(String userId, String ctime, int size, String type) {
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        recall.setType(type);
        List<CorgiVlog> vlogs = corgiVlogService.recallVlog(recall, size);
        for (CorgiVlog vlog : vlogs) {
            vlog.setType("like|");
        }
        return vlogs;
    }

    private List<CorgiVlog> recallRecommendUser(String userId, String ctime) {
        List<UserProfile> userProfiles = corgiUserRecommendService.getVlogRecUser(userId, 100);
        List<CorgiVlog> vlogs = new ArrayList<>();
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        Random random = new Random();
        for (int i = 0; i < userProfiles.size(); i++) {
            int index = random.nextInt(userProfiles.size());
            UserProfile userProfile = userProfiles.get(index);
            if (userProfile == null || StringUtils.isEmpty(userProfile.getUserId())) {
                userProfiles.remove(index);
                continue;
            }
            List<CorgiVlog> vlogList = corgiVlogService.recallTargetVlog(userProfile.getUserId(), recall, 1);
            if (CollectionUtils.isEmpty(vlogList)) {
                userProfiles.remove(index);
                continue;
            }
            vlogs.addAll(vlogList);
            if (vlogs.size() >= 3) {
                break;
            }
        }
        for (CorgiVlog vlog : vlogs) {
            vlog.setType("user|");
        }
        return vlogs;
    }

    private List<CorgiVlog> recallNewVlog(String userId, String ctime) {
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        List<CorgiVlog> vlogs = corgiVlogService.recallVlog(recall, 1);
        for (CorgiVlog vlog : vlogs) {
            vlog.setType("new|");
        }
        return vlogs;
    }

    private List<CorgiVlog> merge(List<CorgiVlog> result, List<CorgiVlog> newVlog, List<String> blackUserIds) {
        if (CollectionUtils.isEmpty(newVlog)) {
            return result;
        }
        Random random = new Random();
        for (CorgiVlog vlog : newVlog) {
            boolean shouldContinue = false;
            if (vlog == null || StringUtils.isEmpty(vlog.getActivityId())) {
                continue;
            }
            if (blackUserIds.contains(vlog.getUserId())) {
                continue;
            }
            for (CorgiVlog olog : result) {
                if (olog.getActivityId().equals(vlog.getActivityId())) {
                    shouldContinue = true;
                    olog.setType(olog.getType() + vlog.getType());
                    break;
                }
            }
            if (shouldContinue) {
                continue;
            }
            int index = random.nextInt(result.size() + 1);
            result.add(index, vlog);
        }
        return result;
    }
}
