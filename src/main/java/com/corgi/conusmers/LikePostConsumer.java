package com.corgi.conusmers;

import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.activity.api.CorgiActivityService;
import com.corgi.activity.entity.CorgiActivity;
import com.corgi.common.CorgiQueueName;
import com.corgi.entity.ActivityQuery;
import com.corgi.user.api.CorgiLikeService;
import com.corgi.user.api.CorgiUserActivityService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.api.CorgiVlogService;
import com.corgi.user.entity.ActivityLike;
import com.corgi.user.entity.CorgiVlogHot;
import com.corgi.user.entity.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
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
    private CorgiUserActivityService corgiUserActivityService;
    @Reference
    private CorgiUserService corgiUserService;
    @Autowired
    private StringRedisTemplate redisTemplate;

    private final List<String> WHITE_LIST = Arrays.asList("744758", "521198", "600670", "528454");

    @RabbitHandler
    public void process(ActivityLike activityLike) {
        log.info("like post init:{} ", activityLike.getActivityId());
        ActivityQuery query = new ActivityQuery();
        query.setUserId(activityLike.getUserId());
        query.setActivityId(activityLike.getActivityId());
        query.setSort("asc");
        query.setPageSize(1);


        Integer likeCount = corgiLikeService.countRealActivityLike(activityLike.getActivityId());
        log.info("like post likeCount:{} ", activityLike.getActivityId());
        if (likeCount >= 10) {
            List<String> preActivityId = corgiUserActivityService.searchFeedActivity(query);
            this.addHot(preActivityId, likeCount * 0.8);

            query.setSort("desc");
            List<String> postActivityId = corgiUserActivityService.searchFeedActivity(query);
            this.addHot(postActivityId, likeCount * 0.8);
        }

    }

    public void addHot(List<String> activityIds, Double likeCount) {
        if (CollectionUtils.isEmpty(activityIds)) {
            return;
        }
        String activityId = activityIds.get(0);
        log.info("like post aroundActivity:{} ", activityId);
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
        Integer expectView = new Double(Math.pow(likeCount, 1.5) * 10 + likeCount * 100).intValue();
        Integer realLikeCount = corgiLikeService.countRealActivityLike(activityId);
        log.info("like post realCount:{} ", realLikeCount);
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
