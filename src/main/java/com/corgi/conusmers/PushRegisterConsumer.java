package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.CorgiBlacklistService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserProfile;
import com.corgi.user.entity.UserQuery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
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
        pushMessage.setSourceUserId(PushService.HELPER);
        pushMessage.setMessage("Corgi终于等到你啦，可小基已经把您的信息推给周边xxx位小哥哥啦，快发些动态展现最美的自己，迎接小哥哥们的招呼吧！");
        pushService.sendMessage(pushMessage);

        UserDetail detail = corgiUserService.getUserDetail(userId, null);
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
        userQuery.setRange(5.0);
        List<String> nearByUsers = corgiUserService.getAllNearByUser(userQuery);
        List<String> resultIds = new ArrayList<>();
        for (String nearByUserId : nearByUsers) {
            if (redisTemplate.opsForValue().setIfAbsent("register_" + nearByUserId, "1", 1L, TimeUnit.DAYS)) {
                resultIds.add(nearByUserId);
            }
        }
        pushMessage.setMessage("可基哟～～你周围又有一位小哥哥注册Corgi啦，快来看看是不是你的菜。");
        pushService.sendMessage(pushMessage, resultIds);
    }


}
