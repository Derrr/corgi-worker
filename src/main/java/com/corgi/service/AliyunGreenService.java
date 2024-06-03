package com.corgi.service;

import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.com.viapi.FileUtils;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.facebody.model.v20191230.RecognizeFaceRequest;
import com.aliyuncs.facebody.model.v20191230.RecognizeFaceResponse;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.corgi.entity.CorgiPic;
import com.corgi.user.api.CorgiPicService;
import com.corgi.user.api.CorgiSoundService;
import com.corgi.user.entity.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

/**
 * @author tairanliu
 */
@Slf4j
@Service
public class AliyunGreenService {

    @Value("${aliyun.accessKeyId}")
    private String accessKeyId;

    @Value("${aliyun.AccessKeySecret}")
    private String accessKeySecret;

    String REGION_ID = "cn-shanghai";

    private IAcsClient managementClient;

    private static final String IMAGE_INFO = "?x-oss-process=image/info";
    private Random random = new Random(System.currentTimeMillis());

    @Reference
    private CorgiPicService corgiPicService;
    @Reference
    private CorgiSoundService corgiSoundService;

    @Autowired
    private StringRedisTemplate redisTemplate;


    @PostConstruct
    void init() {
        IClientProfile profile = DefaultProfile.getProfile(REGION_ID, accessKeyId, accessKeySecret);
        this.managementClient = new DefaultAcsClient(profile);
    }

    public boolean checkFace(CorgiPic pic) {
        log.info("pics = " + pic.getPicUrl());
        RecognizeFaceRequest request = new RecognizeFaceRequest();
        request.setImageURL(this.getUrl(pic.getPicUrl()));
        try {
            RecognizeFaceResponse response = managementClient.getAcsResponse(request);
            RecognizeFaceResponse.Data data = response.getData();
            log.info(JSONObject.toJSONString(data));
            if (data.getFaceCount() > 0) {
                return true;
            }
            return false;
        } catch (ServerException e) {
            log.error(e.getMessage(), e);
            return false;
        } catch (ClientException e) {
            if ("InvalidImage.NotFoundFace".equals(e.getErrCode())) {
                log.info("ErrCode:" + e.getErrCode());
                log.info("ErrMsg:" + e.getErrMsg());
                log.info("RequestId:" + e.getRequestId());
            } else {
                log.error("ErrCode:" + e.getErrCode());
                log.error("ErrMsg:" + e.getErrMsg());
                log.error("RequestId:" + e.getRequestId());
            }
            return false;
        }
    }

    public String getUrl(String url) {
        try {
            FileUtils fileUtils = FileUtils.getInstance(accessKeyId, accessKeySecret);
            return fileUtils.upload(url);
        } catch (ClientException | IOException e) {
            e.printStackTrace();
        }
        return url;
    }


}
