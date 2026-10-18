package com.demo.order.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * 库存服务（storage-service）远程调用客户端
 *
 * 封装 TCC 参与方的三个阶段接口：
 *   POST /storage/try     Try：冻结库存
 *   POST /storage/confirm Confirm：冻结转实扣
 *   POST /storage/cancel  Cancel：解冻归还
 */
@Component
public class StorageClient {

    private static final Logger log = LoggerFactory.getLogger(StorageClient.class);

    private final RestTemplate restTemplate;
    private final String baseUrl;

    public StorageClient(@Value("${storage.service.base-url}") String baseUrl) {
        this.baseUrl = baseUrl;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(factory);
    }

    public TccRemoteResult tryDeduct(String xid, long productId, int count, boolean mockFail) {
        Map<String, Object> body = new HashMap<>();
        body.put("xid", xid);
        body.put("productId", productId);
        body.put("count", count);
        body.put("mockFail", mockFail);
        return post("/storage/try", body);
    }

    public TccRemoteResult confirm(String xid, boolean mockFailConfirm) {
        Map<String, Object> body = new HashMap<>();
        body.put("xid", xid);
        body.put("mockFailConfirm", mockFailConfirm);
        return post("/storage/confirm", body);
    }

    public TccRemoteResult cancel(String xid) {
        Map<String, Object> body = new HashMap<>();
        body.put("xid", xid);
        return post("/storage/cancel", body);
    }

    private TccRemoteResult post(String path, Map<String, Object> body) {
        try {
            ResponseEntity<TccRemoteResult> resp =
                    restTemplate.postForEntity(baseUrl + path, body, TccRemoteResult.class);
            TccRemoteResult r = resp.getBody();
            if (r == null) {
                return new TccRemoteResult(false, "UNKNOWN", "库存服务返回空响应");
            }
            return r;
        } catch (Exception e) {
            log.error("[TCC] 调用库存服务 {} 失败：{}", path, e.getMessage());
            return new TccRemoteResult(false, "UNKNOWN", "调用库存服务失败：" + e.getMessage());
        }
    }
}
