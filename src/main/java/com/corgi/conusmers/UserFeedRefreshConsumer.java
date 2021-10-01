package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.activity.api.CorgiActivityService;
import com.corgi.activity.entity.CorgiActivity;
import com.corgi.common.CorgiConstants;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.MatchRefresher;
import com.corgi.user.api.*;
import com.corgi.user.entity.*;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
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
import java.util.stream.Collectors;

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
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiBlacklistService corgiBlacklistService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;
    @Reference
    private CorgiBarService corgiBarService;
    @Reference
    private CorgiActivityService corgiActivityService;

    @RabbitHandler
    public void process(String userId) {
        log.info("start feeding...{} ", userId);
        if (corgiFeedService.countUnviewFeed(userId) >= 10) {
            return;
        }
        UserDetail userDetail = corgiUserService.getUserDetailBasic(userId);
        if (userDetail == null) {
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
        String groups = null;
        List<String> groupList = corgiUserService.getPreferGroup(userId);
        if (!CollectionUtils.isEmpty(groupList)) {
            groups = String.join("','", groupList);
        }
        String city = userDetail.getCity();
        List<CorgiVlog> result = new ArrayList<>();
        result = merge(result, recallNewVlog(userId, city, groups), blackUserIds);
        result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.AUTO, 2, "asc", groups), blackUserIds);
        result = merge(result, recallRecommendUser(userId, ctime, 3), blackUserIds);
        result = merge(result, recallCity(userId, city), blackUserIds);
        //result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.MANUAL, 1, "asc"), blackUserIds);
        result = merge(result, recallRecommendVlog(userId, ctime, 10 - result.size(), "like"), blackUserIds);
        if (result.size() < 10) {
            result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.AUTO, 10 - result.size(), "desc", groups), blackUserIds);
        }
        if (result.size() < 10) {
            result = merge(result, recallHotVlog(userId, ctime, CorgiVlogHot.TYPE.AUTO, 10 - result.size(), "desc", ""), blackUserIds);
        }
        if (result.size() < 5) {
            result = merge(result, recallNewVlogBySize(userId, groups, 5 - result.size()), blackUserIds);
        }
        if (result.size() < 5) {
            result = merge(result, recallNewVlogBySize(userId, null, 5 - result.size()), blackUserIds);
        }
        for (CorgiVlog vlog : result) {
            corgiFeedService.addFeed(buildFeed(vlog, userId));
        }
    }

    private List<CorgiVlog> recallCity(String userId, String city) {
        if (StringUtils.isEmpty(city)) {
            return new ArrayList<>();
        }
        List<String> barIds = corgiBarService.getBarListByCity(city, null, null)
                .stream().map(bar -> bar.getBarId()).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(barIds)) {
            return new ArrayList<>();
        }
        CorgiVlog recall = new CorgiVlog();
        recall.setUserId(userId);
        recall.setType("id");
        List<CorgiVlog> vlogList = corgiVlogService.recallTargetVlog(String.join("','", barIds), recall, 1);
        for (CorgiVlog vlog : vlogList) {
            vlog.setType("bar|");
        }
        return vlogList;
    }

    private List<CorgiVlog> recallHotVlog(String userId, String ctime, String type, Integer size, String orderby, String groups) {
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setActivityId(groups);
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
        if (CollectionUtils.isEmpty(vlogs)) {
            return this.recallRecommendUser(userId, ctime, size);
        }
        return vlogs;
    }

    private List<CorgiVlog> recallRecommendUser(String userId, String ctime, Integer size) {
        List<UserProfile> userProfiles = corgiUserRecommendService.getVlogRecUser(userId, 100);
        List<CorgiVlog> vlogs = new ArrayList<>();
        CorgiVlog recall = new CorgiVlog();
        recall.setCtime(ctime);
        recall.setUserId(userId);
        recall.setType("like");
        Random random = new Random();
        for (int i = 0; i < userProfiles.size(); i++) {
            if (CollectionUtils.isEmpty(userProfiles)) {
                break;
            }
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
            if (vlogs.size() >= size) {
                break;
            }
            userProfiles.remove(index);
        }
        for (CorgiVlog vlog : vlogs) {
            vlog.setType("user|");
        }
        return vlogs;
    }

    private List<CorgiVlog> recallNewVlogBySize(String userId, String type, Integer size) {
        CorgiVlog recall = new CorgiVlog();
        recall.setUserId(userId);
        recall.setType(type);
        List<CorgiVlog> vlogs = corgiVlogService.recallVlog(recall, size);
        return vlogs;
    }

    private List<CorgiVlog> recallNewVlog(String userId, String city, String type) {
        CorgiVlog recall = new CorgiVlog();
        recall.setUserId(userId);
        recall.setType(type);
        recall.setStatus(city);
        List<CorgiVlog> vlogs = corgiVlogService.recallVlog(recall, 1);
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
