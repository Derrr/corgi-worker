package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.RecommendCalculater;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserRecommendService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserMatch;
import com.corgi.user.entity.UserProfile;
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
@RabbitListener(queues = CorgiQueueName.USER_RECOMMEND_QUEUE)
public class UserRecommendConsumer {
    @Reference
    private CorgiUserService corgiUserService;
    @Reference
    private CorgiUserFollowService corgiUserFollowService;
    @Reference
    private CorgiUserRecommendService corgiUserRecommendService;
    @Autowired
    private StringRedisTemplate redisTemplate;


    @RabbitHandler
    public void process(Channel channel, Message message, RecommendCalculater calculater) {
        String userId = calculater.getUserId();
        log.info("calculating... " + userId);
        if (StringUtils.isEmpty(userId)) {
            return;
        }
        corgiUserRecommendService.clearRecUser(userId);
        int size = 100;
        int page1 = 1;
        List<String> fanIds = new ArrayList<>();
        do {
            List<UserProfile> userProfiles = corgiUserFollowService.getFollowUserByPage(userId, "new", 0.0, 0.0, page1, size);
            if (page1 == 1 && userProfiles.size() < 10) {
                break;
            }
            page1++;
            if (CollectionUtils.isEmpty(userProfiles)) {
                break;
            }
            for (UserProfile userProfile : userProfiles) {
                String followerId = userProfile.getUserId();
                corgiUserRecommendService.followRecUser(userId, followerId);
                int page2 = 1;
                do {
                    List<UserProfile> fanProfiles = corgiUserFollowService.getFollowedUserByPage(followerId, 0L, page2, size);
                    page2++;
                    if (CollectionUtils.isEmpty(fanProfiles)) {
                        break;
                    }
                    for (UserProfile fanProfile : fanProfiles) {
                        String fanId = fanProfile.getUserId();
                        if (fanIds.contains(fanId)) {
                            continue;
                        }
                        fanIds.add(fanId);
                        int page3 = 1;
                        do {
                            List<UserProfile> resultProfiles = corgiUserFollowService.getFollowUserByPage(fanId, "new", 0.0, 0.0, page3, size);
                            page3++;
                            if (CollectionUtils.isEmpty(resultProfiles)) {
                                break;
                            }
                            for (UserProfile resultProfile : resultProfiles) {
                                String resultId = resultProfile.getUserId();
                                corgiUserRecommendService.addRecUser(userId, resultId);
                            }
                        } while (true);
                    }
                } while (true);
            }
        } while (true);

        if (fanIds.size() > 0) {
            corgiUserRecommendService.deleteRecUserByWeight(userId, fanIds.size() / 10);
        }
    }

}
