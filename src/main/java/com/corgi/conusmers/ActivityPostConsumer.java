package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.activity.api.CorgiActivityFeedService;
import com.corgi.activity.api.CorgiActivityService;
import com.corgi.activity.entity.CorgiActivity;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.entity.ActivityQuery;
import com.corgi.service.AliyunGreenService;
import com.corgi.service.PushService;
import com.corgi.user.api.*;
import com.corgi.user.entity.CorgiUserGoods;
import com.corgi.user.entity.CorgiVlogHot;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserProfile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    @Reference
    private CorgiOrderService corgiOrderService;
    @Reference
    private CorgiPicService corgiPicService;
    @Reference
    private CorgiActivityFeedService corgiActivityFeedService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private AliyunGreenService aliyunGreenService;
    @Autowired
    private PushService pushService;


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
//        if (WHITE_LIST.contains(activity.getUserId())) {
//            this.onHot(activity, lockKey);
//            return;
//        }
        if ("influencer".equals(userDetail.getAvatarStatus())) {
            this.onHot(activity, lockKey);
            return;
        }
        String activityId = activity.getId();
        ActivityQuery query = new ActivityQuery();
        query.setPageSize(100);
        query.setUserId(activity.getUserId());
        List<CorgiActivity> corgiActivities = corgiActivityService.getFeedActivity(query);
        if (!CollectionUtils.isEmpty(corgiActivities)) {
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
            if (count == 0) {
                this.preHot(activity, lockKey);
            } else if (max >= 100 || total / count > 10) {
                this.onHot(activity, lockKey);
            } else if (CorgiActivity.CAT_PAYING.equals(activity.getCategory())) {
                query.setCategory(CorgiActivity.CAT_PAYING);
                corgiActivities = corgiActivityService.getFeedActivity(query);
                if (CollectionUtils.isEmpty(corgiActivities)) {
                    this.preHot(activity, lockKey);
                    return;
                }
                CorgiUserGoods goods = new CorgiUserGoods();
                goods.setTraderId(activity.getUserId());
                goods.setGoodsType(CorgiUserGoods.GOODS_TYPE.ACTIVITY);
                goods.setStart(0);
                goods.setSize(500);
                List<CorgiUserGoods> goodsList = corgiOrderService.getUserGoods(goods);
                String descBuyer = "您曾经购买过的付费动态用户 " + userDetail.getNickname() + " 新的付费可见动态，快去查看购买吧！";
                String descFollower = "你关注的好友 " + userDetail.getNickname() + " 发布的付费动态正在被围观快去看看吧！";
                if (!CollectionUtils.isEmpty(goodsList)) {
                    this.preHot(activity, lockKey);
                    List<String> traderIds = goodsList.stream().map(g -> g.getTraderId()).distinct().collect(Collectors.toList());
                    PushMessage followerMessage = buildPayMessage(activity, descFollower);
                    int page = 1;
                    int pageSize = 500;
                    while (true) {
                        List<UserProfile> userProfiles = corgiUserFollowService.getFollowedUserByPage(activity.getUserId(), 0L, page, pageSize);
                        page++;
                        List<String> userIds = new ArrayList<>();
                        for (UserProfile userProfile : userProfiles) {
                            userIds.add(userProfile.getUserId());
                            traderIds.remove(userProfile.getUserId());
                        }
                        pushService.sendMessage(followerMessage, userIds);
                        if (com.alibaba.dubbo.common.utils.CollectionUtils.isEmpty(userProfiles) || userProfiles.size() < pageSize) {
                            break;
                        }
                    }
                    if (!CollectionUtils.isEmpty(traderIds)) {
                        pushService.sendMessage(buildPayMessage(activity, descBuyer), traderIds);
                    }
                }
            }
        }
    }

    private void preHot(CorgiActivity activity, String lockKey) {
        if (!redisTemplate.opsForValue().setIfAbsent(lockKey, System.currentTimeMillis() + "", 10L, TimeUnit.MINUTES)) {
            return;
        }
        if (!aliyunGreenService.checkFace(activity.getPics().get(0))) {
            return;
        }
        CorgiVlogHot corgiVlogHot = new CorgiVlogHot();
        corgiVlogHot.setViewCount(null);
        corgiVlogHot.setLikeCount(0);
        corgiVlogHot.setActivityId(activity.getId());
        corgiVlogHot.setExpectView(1000);
        corgiVlogHot.setType(CorgiVlogHot.TYPE.MANUAL);
        corgiVlogService.addHotVlog(corgiVlogHot);
    }

    private void onHot(CorgiActivity activity, String lockKey) {
        if (!redisTemplate.opsForValue().setIfAbsent(lockKey, System.currentTimeMillis() + "", 5L, TimeUnit.SECONDS)) {
            return;
        }
        CorgiVlogHot corgiVlogHot = new CorgiVlogHot();
        corgiVlogHot.setViewCount(null);
        corgiVlogHot.setLikeCount(0);
        corgiVlogHot.setActivityId(activity.getId());
        corgiVlogHot.setExpectView(3000);
        corgiVlogHot.setType(CorgiVlogHot.TYPE.MANUAL);
        corgiVlogService.addHotVlog(corgiVlogHot);
    }

    private PushMessage buildPayMessage(CorgiActivity activity, String desc) {
        PushMessage pushMessage = new PushMessage();
        pushMessage.setSourceUserId("corgihelper");
        pushMessage.setMessage("热门动态提醒");
        HashMap<String, Object> extra = new HashMap<>();
        extra.put("type", "907");
        extra.put("urlType", "2");
        extra.put("url", activity.getId());
        extra.put("alertTitle", "热门动态提醒");
        if (activity.getCoverUrl() != null && !activity.getCoverUrl().contains("?x-oss-process")) {
            if (StringUtils.isEmpty(activity.getVideoId())) {
                extra.put("picUrl", activity.getCoverUrl() + "?x-oss-process=style/fuzzyCover");
            } else {
                extra.put("picUrl", activity.getCoverUrl());
            }
        }
        extra.put("desc", desc);
        extra.put("showPayReadMask", true);
        pushMessage.setExtra(extra);
        return pushMessage;
    }
}
