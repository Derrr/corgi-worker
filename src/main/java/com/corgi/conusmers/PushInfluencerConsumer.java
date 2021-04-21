package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.*;
import com.corgi.user.entity.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.INFLUENCER_JOIN_QUEUE)
public class PushInfluencerConsumer {

    @Autowired
    private PushService pushService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiBlacklistService corgiBlacklistService;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        String userId = pushMessage.getTargetUserId();
        UserDetail detail = corgiUserService.getUserDetail(userId, null);
        pushMessage.setSourceUserId(PushService.HELPER);
        pushMessage.setMessage("恭喜！成为Corgi的万里挑一的天菜创始人，多发动态及时跟粉丝互动哦，记得私信可小基加入天菜创始人宇宙1群哦，获取更多涨粉秘籍！");
        pushService.sendMessage(pushMessage);

        HashMap<String, Object> extra = new HashMap<>();
        extra.put("type", "907");
        JSONArray content = new JSONArray();
        content.add(new JSONObject().fluentPut("text", "您关注的 "));
        content.add(new JSONObject().fluentPut("text", "@" + detail.getNickname()).fluentPut("url", userId).fluentPut("urlType", "4"));
        content.add(new JSONObject().fluentPut("text", " 成为Corgi万里挑一的天菜创始人啦，快给他点个赞沾沾喜气，快让他给你分享下天菜创始人的修炼秘密吧。"));
        content.add(new JSONObject().fluentPut("text", "看看他>").fluentPut("url", userId).fluentPut("urlType", "4"));
        extra.put("content", content);
        pushMessage.setExtra(extra);
        pushMessage.setMessage("您关注的人成为Corgi万里挑一的天菜创始人啦，快给他点个赞沾沾喜气，快让他给你分享下天菜创始人的修炼秘密吧");
        int page = 1;
        while (true) {
            List<UserProfile> fans = corgiUserFollowService.getFollowedUserByPage(userId, 0l, page, 300);
            if (CollectionUtils.isEmpty(fans)) {
                break;
            }
            List<String> userIds = new ArrayList<>();
            for (UserProfile fan : fans) {
                userIds.add(fan.getUserId());
            }
            pushService.sendMessage(pushMessage, userIds);
            page++;
        }
        Double lat = detail.getLat();
        Double lng = detail.getLng();
        if (lat > 200) {
            return;
        }
        if (lng > 200) {
            return;
        }
        UserQuery userQuery = new UserQuery();
        userQuery.setLat(lat);
        userQuery.setLng(lng);
        userQuery.setUserId(userId);
        userQuery.setRange(30.0);
        List<String> nearByUsers = corgiUserService.getAllNearByUser(userQuery);
        List<String> resultIds = new ArrayList<>();
        for (String nearByUserId : nearByUsers) {
            if (corgiUserFollowService.isFollowed(userId, nearByUserId) < 2) {
                if (corgiBlacklistService.isBlacked(userId, nearByUserId) == 0) {
                    resultIds.add(nearByUserId);
                }
            }
        }
        content = new JSONArray();
        content.add(new JSONObject().fluentPut("text", "可基哟～～你周围又诞生了一位天菜创始人，快去基达地图上康康他是不是你的菜。"));
        content.add(new JSONObject().fluentPut("text", "看看他>").fluentPut("url", userId).fluentPut("urlType", "4"));
        extra.put("content", content);
        pushMessage.setExtra(extra);
        pushMessage.setMessage("可基哟～～你周围又诞生了一位天菜创始人，快去基达地图上康康他是不是你的菜。");
        pushService.sendMessage(pushMessage, resultIds);
    }


}
