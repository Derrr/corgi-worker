package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiConstants;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.MatchRefresher;
import com.corgi.user.api.CorgiUserMatchService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserDetail;
import com.corgi.user.entity.UserMatch;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.REFRESH_MATCH_QUEUE)
public class UserMatchRefreshConsumer {
    @Reference
    private CorgiUserMatchService corgiUserMatchService;
    @Reference
    private CorgiUserService corgiUserService;
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(Channel channel, Message message, MatchRefresher refresher) {
        String userId = refresher.getUserId();
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        int size = 1000;
        int start = 0;
        List<UserMatch> userMatchList;
        UserDetail loginUserDetail = corgiUserService.getUserDetail(userId, null);
        do {
            userMatchList = corgiUserMatchService.getUserMatchByPage(userId, start, size);
            start += size;
            if (!CollectionUtils.isEmpty(userMatchList)) {
                for (UserMatch userMatch : userMatchList) {
                    UserDetail userDetail1 = getUserDetail(userId, userMatch.getUserId1(), loginUserDetail);
                    UserDetail userDetail2 = getUserDetail(userId, userMatch.getUserId2(), loginUserDetail);
                    if (userDetail1 == null || userDetail2 == null) {
                        continue;
                    }
                    Double match = corgiUserMatchService.calculateUserMatchByDetail(userDetail1, userDetail2);
                    userMatch.setMatch(match);
                    corgiUserMatchService.updateMatch(userMatch);
                    String matchKey = CorgiConstants.getUserMatchKey(userDetail1.getUserId(), userDetail2.getUserId());
                    Long expire = redisTemplate.getExpire(matchKey);
                    if (expire > 0) {
                        redisTemplate.opsForValue().set(matchKey, match.toString(), expire);
                    }
                }
            } else {
                break;
            }
        } while (true);
    }

    private UserDetail getUserDetail(String loginUserId, String userId, UserDetail loginUserDetail) {
        if (!loginUserId.equals(userId)) {
            return corgiUserService.getUserDetail(userId, null);
        }
        return loginUserDetail;
    }
}
