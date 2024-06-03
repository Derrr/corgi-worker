package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.activity.api.CorgiActivityService;
import com.corgi.activity.entity.CorgiActivity;
import com.corgi.common.CorgiQueueName;
import com.corgi.entity.ActivityQuery;
import com.corgi.user.api.*;
import com.corgi.user.entity.ActivityLike;
import com.corgi.user.entity.CorgiVlogHot;
import com.corgi.user.entity.UserBasic;
import com.corgi.user.entity.UserDetail;
import lombok.extern.slf4j.Slf4j;
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
@RabbitListener(queues = CorgiQueueName.LIKE_POST_QUEUE)
public class LikePostConsumer {
    @Reference
    private CorgiLikeService corgiLikeService;
    @Reference
    private CorgiVlogService corgiVlogService;
    @Reference
    private CorgiFeedService corgiFeedService;
    @Reference
    private CorgiActivityService corgiActivityService;
    @Reference
    private CorgiUserActivityService corgiUserActivityService;
    @Reference
    private CorgiBlacklistService corgiBlacklistService;
    @Reference
    private CorgiUserService corgiUserService;
    @Autowired
    private StringRedisTemplate redisTemplate;

    private final List<String> WHITE_LIST = Arrays.asList("744758", "521198", "600670", "528454");

    @RabbitHandler
    public void process(ActivityLike activityLike) {
        log.info("post like:{} ", activityLike.getActivityId());
        ActivityQuery query = new ActivityQuery();
        query.setUserId(activityLike.getUserId());
        query.setActivityId(activityLike.getActivityId());
        query.setSort("asc");
        query.setPageSize(1);

        Integer likeCount = corgiLikeService.countRealActivityLike(activityLike.getActivityId());
        log.info("like count:{} {} ", activityLike.getActivityId(), likeCount);
        if (likeCount >= 30) {
            List<String> preActivityId = corgiUserActivityService.searchFeedActivity(query);
            this.addHot(preActivityId, likeCount * 0.8);

            query.setSort("desc");
            List<String> postActivityId = corgiUserActivityService.searchFeedActivity(query);
            this.addHot(postActivityId, likeCount * 0.8);
        }

        if (likeCount > 10) {
            String key = activityLike.getActivityId() + "-recommend";
            if (!redisTemplate.hasKey(key)) {
                this.refreshRecommendList(activityLike.getActivityId(), likeCount, key);
            }
            List<String> recommendIds = redisTemplate.opsForList().range(key, 0, -1);
            String loginUserId = activityLike.getLikeUserId();
            List<String> blackIds = this.getBlackIds(loginUserId);
            String userKey = loginUserId + "-recommend-activity";
            List<CorgiActivity> activities = corgiActivityService.getActivityByIds(recommendIds);
            for (CorgiActivity activity : activities) {
                if (corgiFeedService.countFeed(activity.getId(), loginUserId) > 0) {
                    continue;
                }
                if (StringUtils.isEmpty(activity.getUserId())) {
                    continue;
                }
                if (loginUserId.equals(activity.getUserId())) {
                    continue;
                }
                if (blackIds.contains(activity.getUserId())) {
                    continue;
                }
                redisTemplate.opsForList().leftPush(userKey, activity.getId() + "-" + activity.getUserId());
                log.info("add recommend:{} {}", activityLike.getActivityId(), activity.getId());
            }
            redisTemplate.expire(userKey, 90, TimeUnit.DAYS);
        }
    }

    private void refreshRecommendList(String activityId, Integer likeCount, String key) {
        int page = 1;
        Map<String, Integer> map = new HashMap<>();
        while (true) {
            List<ActivityLike> likes = corgiLikeService.getActivityLike(activityId, page, 100);
            if (CollectionUtils.isEmpty(likes)) {
                break;
            }
            for (ActivityLike like : likes) {
                List<String> activityIds = corgiLikeService.getLikedActivity(like.getLikeUserId(), "", "", 1, 1000);
                log.info("like user:{} count:{} ", like.getLikeUserId(), activityIds.size());
                for (String id : activityIds) {
                    if (id.equals(activityId)) {
                        continue;
                    }
                    Integer score = map.get(id);
                    if (score == null) {
                        score = 0;
                    }
                    score++;
                    map.put(id, score);
                }
            }
            page++;
        }
        List<Map.Entry> entryList = new ArrayList<>();
        int minValue = 0;
        int threshold = likeCount / 10 + 10;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entryList.size() < threshold) {
                if (!checkActivity(entry.getKey())) {
                    continue;
                }
                entryList.add(entry);
                if (entry.getValue() < minValue) {
                    minValue = entry.getValue();
                }
            } else if (entry.getValue() > minValue) {
                if (!checkActivity(entry.getKey())) {
                    continue;
                }
                int tmpMinValue = entry.getValue();
                int minIndex = 0;
                for (int i = 0; i < entryList.size(); i++) {
                    Map.Entry<String, Integer> oldEntry = entryList.get(i);
                    if (oldEntry.getValue() < minValue) {
                        minIndex = i;
                        tmpMinValue = minValue;
                        minValue = oldEntry.getValue();
                    }
                    if (oldEntry.getValue() == minValue) {
                        minIndex = i;
                    }
                    if (oldEntry.getValue() > minValue && oldEntry.getValue() < tmpMinValue) {
                        tmpMinValue = oldEntry.getValue();
                    }
                }
                entryList.remove(minIndex);
                entryList.add(entry);
                minValue = tmpMinValue;
            }
        }
        for (Map.Entry<String, Integer> entry : entryList) {
            redisTemplate.opsForList().leftPush(key, entry.getKey());
            log.info("activity recommend:{}", entry.getKey());
        }
        redisTemplate.expire(key, likeCount / 30 + 1, TimeUnit.DAYS);
    }

    private boolean checkActivity(String activityId) {
        String status = corgiUserActivityService.getStatus("", activityId);
        if (StringUtils.isEmpty(status)) {
            return false;
        }
        return "normal".equals(status) || "pass".equals(status);
    }

    private List<String> getBlackIds(String userId) {
        String blackKey = "black_cache_" + userId;
        List<String> blackUserIds = new ArrayList<>();
        if (!redisTemplate.hasKey(blackKey)) {
            List<UserBasic> basicList = corgiBlacklistService.getBlackUser(userId);
            List<String> beBlackedIds = corgiBlacklistService.getBeBlacked(userId);
            if (!CollectionUtils.isEmpty(basicList)) {
                for (UserBasic basic : basicList) {
                    blackUserIds.add(basic.getUserId());
                }
            }
            if (!CollectionUtils.isEmpty(beBlackedIds)) {
                blackUserIds.addAll(beBlackedIds);
            }
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, -30);
            blackUserIds.addAll(corgiBlacklistService.getUninterestedCreator(userId, new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime())));
            if (CollectionUtils.isEmpty(blackUserIds)) {
                redisTemplate.delete(blackKey);
                redisTemplate.opsForList().leftPush(blackKey, "null");
            } else {
                redisTemplate.opsForList().leftPushAll(blackKey, blackUserIds);
            }
            redisTemplate.expire(blackKey, 1L, TimeUnit.DAYS);
        } else {
            blackUserIds = redisTemplate.opsForList().range(blackKey, 0, -1);
            if (blackUserIds.size() == 1 && "null".equals(blackUserIds.get(0))) {
                return new ArrayList<>();
            }
        }
        return blackUserIds;
    }

    public void addHot(List<String> activityIds, Double likeCount) {
        if (CollectionUtils.isEmpty(activityIds)) {
            return;
        }
        String activityId = activityIds.get(0);
        CorgiVlogHot hot = new CorgiVlogHot();
        hot.setActivityId(activityId);

        CorgiVlogHot queryHot = new CorgiVlogHot();
        queryHot.setStatus(CorgiVlogHot.STATUS.OPEN);
        queryHot.setType(CorgiVlogHot.TYPE.AUTO);
        queryHot.setActivityId(activityId);
        List<CorgiVlogHot> tmpList = corgiVlogService.getHotVlog(queryHot, 1, 1);
        if (tmpList.size() > 0) {
            hot = tmpList.get(0);
        }
        Integer expectView = 500 + new Double(likeCount * 90).intValue();
        Integer realLikeCount = corgiLikeService.countRealActivityLike(activityId);
        log.info("post like expect:{} real:{} ", expectView, realLikeCount);
        if (hot.getId() == null) {
            CorgiVlogHot addHot = new CorgiVlogHot();
            addHot.setActivityId(hot.getActivityId());
            addHot.setExpectView(expectView);
            addHot.setLikeCount(realLikeCount);
            addHot.setType(CorgiVlogHot.TYPE.AUTO);
            corgiVlogService.addHotVlog(addHot);
        } else if (expectView > hot.getExpectView()) {
            CorgiVlogHot updateHot = new CorgiVlogHot();
            updateHot.setId(hot.getId());
            updateHot.setLikeCount(realLikeCount);
            updateHot.setExpectView(expectView);
            corgiVlogService.updateHotVlog(updateHot);
        }
    }
}
