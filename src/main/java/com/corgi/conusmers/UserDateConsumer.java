package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.service.PushService;
import com.corgi.user.api.CorgiUserDateService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.CorgiDate;
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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.USER_DATE_QUEUE)
public class UserDateConsumer {
    @Reference
    private CorgiUserDateService corgiUserDateService;
    @Reference
    private CorgiUserService corgiUserService;
    @Autowired
    private PushService pushService;
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(PushMessage pushMessage) {
        pushService.sendMessage(pushMessage);
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        CorgiDate search = new CorgiDate();
//        search.setCity(corgiDate.getCity());
//        search.setStatus("open");
//        if (!"不限".equals(corgiDate.getType())) {
//            search.setType(corgiDate.getType());
//        }
//        if (!"0".equals(corgiDate.getPayType())) {
//            search.setPayType(getPayType(corgiDate.getPayType()));
//        }
//        if (!"0".equals(corgiDate.getEndTime())) {
//            search.setEndTime(sdf.format(new Date()));
//            try {
//                Integer size = Integer.parseInt(corgiDate.getEndTime());
//                Calendar calendar = Calendar.getInstance();
//                calendar.add(Calendar.DATE, size);
//                corgiDate.setEndTime(sdf.format(calendar.getTime()));
//            } catch (Exception e) {
//                log.error(e.getMessage(), e);
//            }
//        }
//        List<CorgiDate> dates = corgiUserDateService.searchDate(search);
//        log.info("dates:{} ", dates);
//        if (!CollectionUtils.isEmpty(dates)) {
//            for (CorgiDate date : dates) {
//                String takenUser = date.getUserId();
//                if (!getLock(takenUser)) {
//                    continue;
//                }
//                try {
//                    CorgiDate takenDate = corgiUserDateService.getDateById(date.getId());
//                    if (!"open".equals(takenDate.getStatus())) {
//                        continue;
//                    }
//                    takenDate.setStatus("taken");
//                    corgiUserDateService.updateDate(takenDate);
//                    sendMessage(takenDate);
//
//                    corgiDate.setStatus("taken");
//                    corgiUserDateService.updateDate(corgiDate);
//                    sendMessage(corgiDate);
//                } finally {
//                    deleteLock(takenUser);
//                }
//            }
//        }
//        log.info("date:{} ", corgiDate);
//        corgiUserDateService.addDate(corgiDate);
    }

    private void sendMessage(CorgiDate corgiDate) {
//        PushMessage pushMessage = new PushMessage();
//        pushMessage.setMessage("约会成功！");
//        pushMessage.setSourceUserId(PushService.HELPER);
//        pushMessage.setTargetUserId(corgiDate.getTakenUser());
//        HashMap<String, String> extra = new HashMap<>();
//        UserDetail userDetail = corgiUserService.getUserDetailBasic(corgiDate.getUserId());
//        extra.put("type", "907");
//        extra.put("userId", corgiDate.getUserId());
//        extra.put("avatarUrl", userDetail.getAvatar());
//        extra.put("nickname", userDetail.getNickname());
//        pushMessage.setExtra(extra);
//        pushService.sendMessage(pushMessage);
    }

    private boolean getLock(String userId) {
        for (int i = 0; i < 10; i++) {
            if (redisTemplate.opsForValue().setIfAbsent("user_date_lock_" + userId, "1", 10L, TimeUnit.SECONDS)) {
                return true;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private void deleteLock(String userId) {
        redisTemplate.delete("user_date_lock_" + userId);
    }

    private String getPayType(String payType) {
        if ("1".equals(payType)) {
            return "3";
        }
        if ("3".equals(payType)) {
            return "1";
        }
        return payType;
    }
}
