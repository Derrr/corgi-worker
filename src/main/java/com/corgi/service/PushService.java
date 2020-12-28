package com.corgi.service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.corgi.common.messages.PushMessage;
import com.corgi.user.api.CorgiUserService;
import com.corgi.user.entity.SystemMessage;
import com.corgi.user.entity.UserLogin;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author tairanliu
 */
@Slf4j
@Service
public class PushService {
    private String orgName = "1101200130181163";
    private String appName = "corgi";
    private static final String HOST = "https://a1.easemob.com/";
    private static final String MESSAGE_URL = "/messages";
    private static final String TOKEN_URL = "/token";
    private final static PoolingHttpClientConnectionManager poolConnManager = new PoolingHttpClientConnectionManager();
    public static ThreadLocal<String> RESULT = new ThreadLocal<>();
    public static final String HELPER = "corgihelper";

    @Autowired
    private StringRedisTemplate redisTemplate;

    @PostConstruct
    public void init() {
        poolConnManager.setMaxTotal(2000);
        poolConnManager.setDefaultMaxPerRoute(1000);
    }

    private CloseableHttpClient getCloseableHttpClient() {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(poolConnManager)
                .setRetryHandler(new DefaultHttpRequestRetryHandler())
                .build();

        return httpClient;
    }

    public void sendMessage(PushMessage pushMessage) {
        sendMessage(pushMessage, null);
    }

    public String sendMessage(PushMessage pushMessage, List<String> userIds) {
        HashMap extra = pushMessage.getExtra();
        if (extra == null) {
            extra = new HashMap();
        }
        if (CollectionUtils.isEmpty(userIds)) {
            userIds = Arrays.asList("corgi" + pushMessage.getTargetUserId());
        } else {
            List<String> tmpUserIds = new ArrayList<>();
            for (String userId : userIds) {
                tmpUserIds.add("corgi" + userId);
            }
            userIds = tmpUserIds;
        }
        String url = HOST + orgName + "/" + appName + MESSAGE_URL;
        HashMap message = new HashMap();
        if(HELPER.equals(pushMessage.getSourceUserId())){
            message.put("from", pushMessage.getSourceUserId());
        }
        message.put("target_type", "users");
        message.put("target", userIds);
        HashMap msg = new HashMap();
        try {
            HashMap apnsContent = new HashMap();
            apnsContent.put("em_push_content",new String(pushMessage.getMessage().getBytes(),"UTF-8"));
            extra.put("em_apns_ext", apnsContent);
            msg.put("msg", new String(pushMessage.getMessage().getBytes(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            log.error(e.getMessage(), e);
        }
        msg.put("type", "txt");
        message.put("msg", msg);
        message.put("ext", extra);
        try {
            String accessToken = getToken();
            return this.postJson(url, message, accessToken);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return "false";
    }

    public String getToken() {
        String tokenKey = "HX_TOKEN";
        String token = redisTemplate.opsForValue().get(tokenKey);
        if (StringUtils.isEmpty(token)) {
            String url = HOST + orgName + "/" + appName + TOKEN_URL;
            HashMap message = new HashMap();
            message.put("grant_type", "client_credentials");
            message.put("client_id", "YXA6NW6WhxTlSd6PW28d8s2geQ");
            message.put("client_secret", "YXA6bXC8NAPVUHKlxTlhCSSZOVwyiAQ");
            String result = this.postJson(url, message, null);
            JSONObject object = JSONObject.parseObject(result);
            token = object.getString("access_token");
            redisTemplate.opsForValue().set(tokenKey, token, 5, TimeUnit.MINUTES);
        }
        return token;
    }

    public String postJson(String url, HashMap message, String token) {
        String result = null;
        CloseableHttpClient httpClient = getCloseableHttpClient();
        HttpPost httpPost = new HttpPost(url);
        CloseableHttpResponse response = null;
        try {

            httpPost.setHeader("Accept", "application/json;charset=UTF-8");
            httpPost.setHeader("Content-Type", "application/json");
            if (!StringUtils.isEmpty(token)) {
                httpPost.setHeader("Authorization", "Bearer " + token);
            }
            log.info("request: {} ", JSONObject.toJSONString(message));
            StringEntity stringEntity = new StringEntity(JSONObject.toJSONString(message),Charset.forName("UTF-8"));
            stringEntity.setContentType("application/json;charset=UTF-8");

            httpPost.setEntity(stringEntity);
            response = httpClient.execute(httpPost);
            if (response != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                result = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
                log.info("请求成功：{}", result);
                response.getEntity().getContent().close();
                return result;
            } else if (response != null) {
                result = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
                log.error("请求 {} 获取失败, 状态异常：{} 返回结果: {}", url, response.getStatusLine().getStatusCode(), result);
            }

        } catch (IOException e) {
            log.error("请求地址出错," + url + "错误信息:", e);
            result = e.getMessage();
            httpPost.abort();
        } catch (IllegalArgumentException e) {
            log.error("返回参数错误", e);
            result = e.getMessage();
            httpPost.abort();
        } finally {
            if (response != null) {
                try {
                    EntityUtils.consume(response.getEntity());
                    response.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        RESULT.set(result);
        return "false";
    }

//    public void sendMessage(PushMessage pushMessage, List<String> userIds) {
//        String message = pushMessage.getMessage();
//        HashMap<String, Object> extras = pushMessage.getExtra();
//        if (extras != null) {
//            for (Map.Entry entry : extras.entrySet()) {
//                entry.setValue(entry.getValue() + "");
//            }
//        }
//        PushPayload payload = null;
//        if (CollectionUtils.isEmpty(userIds) && pushMessage.getTargetUserId() != null) {
//            String userId = pushMessage.getTargetUserId();
//            log.info("send to: " + userId);
//            UserLogin userLogin = corgiUserService.getUserLogin(userId);
//            if (userLogin != null && userLogin.getImId() != null) {
//                payload = getPayload(userLogin.getImId(), message, extras);
//            }
//        } else if (CollectionUtils.isNotEmpty(userIds)) {
//            payload = getPayload(userIds, message, extras);
//        }
//        if (payload != null) {
//            try {
//                for (int i = 0; i < 10; i++) {
//                    PushResult result = jpushClient.sendPush(payload);
//                    PushResult.Error error = result.error;
//                    log.info("send message: {}, result: {}", payload, result);
//                    if (error == null || error.getCode() != 2002) {
//                        break;
//                    }
//                    try {
//                        Thread.sleep(500L);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            } catch (APIConnectionException e) {
//                e.printStackTrace();
//            } catch (APIRequestException e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    private PushPayload getPayload(String registrationId, String message, HashMap extras) {
//        if (extras == null) {
//            extras = new HashMap();
//        }
//        return PushPayload.newBuilder()
//                .setPlatform(Platform.all())
//                .setAudience(Audience.registrationId(registrationId))
//                .setMessage(Message.newBuilder().setMsgContent(message)
//                        .addExtras(extras).build())
//                .setNotification(Notification.newBuilder()
//                        .addPlatformNotification((AndroidNotification.newBuilder().setAlert(message).addExtras(extras))
//                                .setTitle("corgi").build())
//                        .addPlatformNotification((IosNotification.newBuilder().setAlert(message).addExtras(extras))
//                                .build())
//                        .build())
//                .build();
//    }
//
//    private PushPayload getPayload(List<String> registrationIds, String message, HashMap extras) {
//        if (extras == null) {
//            extras = new HashMap();
//        }
//        return PushPayload.newBuilder()
//                .setPlatform(Platform.all())
//                .setAudience(Audience.registrationId(registrationIds))
//                .setMessage(Message.newBuilder().setMsgContent(message)
//                        .addExtras(extras).build())
//                .build();
//    }
}
