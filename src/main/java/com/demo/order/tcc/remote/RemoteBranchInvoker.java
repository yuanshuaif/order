package com.demo.order.tcc.remote;

import com.demo.order.client.TccRemoteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * 远程分支通用调用器：对任何动作、任何阶段，统一 POST 到参与方的 {serviceUrl}/tcc/invoke。
 *
 * 请求是统一信封 {xid, actionName, phase, params}；参与方按注册的方法分发执行。
 * 发起方不再为每个参与方写一个 Client 类——StorageClient 由此退役。
 */
@Component
public class RemoteBranchInvoker {

    private static final Logger log = LoggerFactory.getLogger(RemoteBranchInvoker.class);

    private final RestTemplate restTemplate;

    public RemoteBranchInvoker() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(factory);
    }

    /**
     * @param action 注册表里查到的动作信息（提供 serviceUrl / actionName）
     * @param phase  TRY / CONFIRM / CANCEL
     * @param params 业务参数（含测试开关），按参数名与参与方方法形参绑定
     */
    public TccRemoteResult invoke(TccActionInfo action, String xid, String phase, Map<String, Object> params) {
        Map<String, Object> envelope = new HashMap<>();
        envelope.put("xid", xid);
        envelope.put("actionName", action.getActionName());
        envelope.put("phase", phase);
        envelope.put("params", params == null ? new HashMap<String, Object>() : params);
        try {
            ResponseEntity<TccRemoteResult> resp = restTemplate.postForEntity(
                    action.getServiceUrl() + "/tcc/invoke", envelope, TccRemoteResult.class);
            TccRemoteResult r = resp.getBody();
            if (r == null) {
                return new TccRemoteResult(false, "UNKNOWN", "参与方返回空响应");
            }
            return r;
        } catch (Exception e) {
            log.error("[Invoker] 调用动作 {} 阶段 {} 失败：{}", action.getActionName(), phase, e.getMessage());
            return new TccRemoteResult(false, "UNKNOWN",
                    "调用参与方失败（" + action.getServiceName() + "）：" + e.getMessage());
        }
    }
}
