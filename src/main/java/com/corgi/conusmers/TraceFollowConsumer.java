package com.corgi.conusmers;

import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.CorgiQueueName;
import com.corgi.common.messages.PushMessage;
import com.corgi.common.messages.TraceFollow;
import com.corgi.service.PushService;
import com.corgi.user.api.CorgiStatisticService;
import com.corgi.user.api.CorgiUserFollowService;
import com.corgi.user.api.CorgiUserMatchService;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserProfile;
import com.corgi.user.entity.UserQuery;
import com.corgi.user.entity.UserTrace;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Component
@Slf4j
@RabbitListener(queues = CorgiQueueName.TRACE_FOLLOW_QUEUE)

public class TraceFollowConsumer {
    private static final long MAX_INTERVAL = 5 * 60 * 1000L;

    private static final double MIN_INTERVAL = 0.5 * 60 * 1000;

    @Reference
    private CorgiStatisticService corgiStatisticService;

    @RabbitHandler
    public void process(TraceFollow traceFollow) {
        if (StringUtils.isEmpty(traceFollow.getUserId())) {
            log.error("trace userId 为空");
            return;
        }

        if (TraceFollow.CHANGE.equals(traceFollow.getOption())) {
            changeUserTrace(traceFollow);
        } else {
            countUserTrace(traceFollow);
        }
    }

    void changeUserTrace(TraceFollow traceFollow) {
        UserTrace lastTrace = this.getLastUserTrace(traceFollow.getUserId());
        long now = System.currentTimeMillis();
        if (lastTrace == null) {
            addTrace(traceFollow, now);
        } else if (!lastTrace.getType().equals(traceFollow.getType()) || (now - lastTrace.getUptime()) > MAX_INTERVAL) {
            skipTrace(lastTrace, traceFollow.getType());
            addTrace(traceFollow, now);
        }
    }

    void countUserTrace(TraceFollow traceFollow) {
        long now = System.currentTimeMillis();
        UserTrace lastTrace = this.getLastUserTrace(traceFollow.getUserId());
        if (lastTrace == null) {
            return;
        }
        if (now - lastTrace.getUptime() > MIN_INTERVAL) {
            lastTrace.setStayCount(lastTrace.getStayCount() + 1);
            lastTrace.setUptime(now);
            corgiStatisticService.updateTraceStay(lastTrace);
        }
    }

    void addTrace(TraceFollow traceFollow, long now) {
        UserTrace trace = UserTrace.builder()
                .uptime(now)
                .userId(traceFollow.getUserId())
                .stayCount(1L)
                .type(traceFollow.getType())
                .build();
        corgiStatisticService.addUserTrace(trace);
    }

    UserTrace getLastUserTrace(String userId) {
        List<UserTrace> userTraces = corgiStatisticService.getLastUserTrace(userId);
        if (CollectionUtils.isEmpty(userTraces)) {
            return null;
        }
        if (userTraces.size() == 1) {
            return userTraces.get(0);
        }
        UserTrace result = userTraces.get(0);
        for (int i = 1; i < userTraces.size(); i++) {
            UserTrace trace = userTraces.get(i);
            UserTrace lastTrace = userTraces.get(i - 1);
            skipTrace(trace, lastTrace.getType());
        }
        return result;
    }

    void skipTrace(UserTrace lastTrace, String nextType) {
        lastTrace.setNextType(nextType);
        lastTrace.setStayStatus(UserTrace.CHANGED);
        corgiStatisticService.updateTraceStatus(lastTrace);
    }
}
