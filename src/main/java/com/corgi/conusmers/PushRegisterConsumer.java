package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.CorgiBlacklistService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserProfile;
import com.corgi.user.entity.UserQuery;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.REGISTER_QUEUE)
public class PushRegisterConsumer {

    @Autowired
    private PushService pushService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserService corgiUserService;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @RabbitHandler
    public void process(PushMessage pushMessage) {
        String userId = pushMessage.getTargetUserId();
        UserDetail detail = corgiUserService.getUserDetail(userId, null);
        if (detail == null) {
            log.error("注册查无此人:{} ", userId);
            return;
        }
//        Double lat = detail.getLat();
//        Double lng = detail.getLng();
//        if (lat > 200) {
//            return;
//        }
//        if (lng > 200) {
//            return;
//        }
//        UserQuery userQuery = new UserQuery();
//        userQuery.setLat(lat);
//        userQuery.setLng(lng);
//        userQuery.setUserId(userId);
//        userQuery.setRange(5.0);
//        List<String> nearByUsers = corgiUserService.getAllNearByUser(userQuery);
//        List<String> resultIds = new ArrayList<>();
//        for (String nearByUserId : nearByUsers) {
//            if (redisTemplate.opsForValue().setIfAbsent("register_" + nearByUserId, "1", 1L, TimeUnit.DAYS) && !nearByUserId.equals(userId)) {
//                resultIds.add(nearByUserId);
//            }
//        }
        pushMessage.setSourceUserId(PushService.CORGI_HELPER);
        pushMessage.setMessage("1分钟解锁Corgi流量密码！「新手必看」");
        HashMap<String, Object> extra = new HashMap<>();
        extra.put("type", "905");
//        JSONArray content = new JSONArray();
//        content.add(new JSONObject().fluentPut("text", "可基哟～～你周围又有一位小哥哥 "));
//        content.add(new JSONObject().fluentPut("text", "@" + detail.getNickname()).fluentPut("url", detail.getUserId()).fluentPut("urlType", "4"));
//        content.add(new JSONObject().fluentPut("text", " 注册Corgi啦，快来看看是不是你的菜。"));
//        content.add(new JSONObject().fluentPut("text", " 看看他>>").fluentPut("url", detail.getUserId()).fluentPut("urlType", "4"));
//        extra.put("content", "如何获得更多流量推荐？如何可以上corgi封面？天菜创始人又是什么？点击查看详情攻略。");
        extra.put("title", "1分钟解锁Corgi流量密码！「新手必看」");
        extra.put("desc", "如何获得更多流量推荐？如何可以上corgi封面？天菜创始人又是什么？点击查看详情攻略。");
        extra.put("picUrl", "https://corgi-pic.oss-cn-beijing.aliyuncs.com/corgi/newer.jpeg");
        extra.put("urlType", "1");
        extra.put("url", "https://www.corgi.org.cn/html/wechat/html2user/index.html");
        pushMessage.setExtra(extra);
        pushService.sendMessage(pushMessage, Arrays.asList(userId));
//        if (resultIds.size() > 0) {
//            pushMessage.setExtra(new HashMap());
//            pushMessage.setMessage("Corgi终于等到你啦，可小基已经把您的信息推给周边 " + resultIds.size() + " 位小哥哥啦，快发些动态展现最美的自己，迎接小哥哥们的招呼吧！");
//            pushService.sendMessage(pushMessage);
//        }
    }


}
