package com.corgi.service;

import cn.jiguang.common.resp.APIConnectionException;
import cn.jiguang.common.resp.APIRequestException;
import cn.jpush.api.JPushClient;
import cn.jpush.api.push.PushResult;
import cn.jpush.api.push.model.Message;
import cn.jpush.api.push.model.Platform;
import cn.jpush.api.push.model.PushPayload;
import cn.jpush.api.push.model.audience.Audience;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.messages.PushMessage;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.UserLogin;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;

/**
 * @author tairanliu
 */
@Slf4j
@Service
public class PushService {
    @Autowired
    private JPushClient jpushClient;
    @Reference
    private CorgiUserService corgiUserService;

    public void sendMessage(PushMessage pushMessage) {
        String userId = pushMessage.getTargetUserId();
        String message = pushMessage.getMessage();
        HashMap extras = pushMessage.getExtra();
        UserLogin userLogin = corgiUserService.getUserLogin(userId);
        if (userLogin != null && userLogin.getImId() != null) {
            PushPayload payload = getPayload(userLogin.getImId(), message, extras);
            try {
                PushResult result = jpushClient.sendPush(payload);
                log.info("send from {} to {}: result={}", pushMessage.getSourceUserId(), userId, result);
            } catch (APIConnectionException e) {
                e.printStackTrace();
            } catch (APIRequestException e) {
                e.printStackTrace();
            }
        }
    }

    private PushPayload getPayload(String registrationId, String message, HashMap extras) {
        if(extras == null){
            extras = new HashMap();
        }
        return PushPayload.newBuilder()
                .setPlatform(Platform.all())
                .setAudience(Audience.registrationId(registrationId))
                .setMessage(Message.newBuilder().setMsgContent(message)
                        .addExtras(extras).build())
                .build();
    }

}
