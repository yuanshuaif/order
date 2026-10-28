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
 * 远程分支调用器：两副面孔，对应 TCC 的两个阶段。
 *
 * 一阶段（Try）：直调动作的专属业务端点 POST {serviceUrl}/tcc/{actionName}，
 *   请求体是扁平业务参数——发起方对 Try 就是一次正常的业务调用，无信封无反射。
 * 二阶段（Confirm/Cancel）：统一信封 POST {serviceUrl}/tcc/invoke，
 *   {xid, actionName, phase, params}，由参与方反射分发到派生的伴生方法
 *   （对应 Seata TC 回调 RM 后 TCCResourceManager 的反射执行）。
 *
 * 发起方不为任何参与方写 Client 类：URL 由动作名按约定生成，方法由注册表与参与方自决。
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
     * 一阶段：直调动作的业务端点（URL 约定 POST /tcc/{动作名}），body 为扁平业务参数。
     * @param action 注册表里查到的动作信息（提供 serviceUrl / actionName）
     * @param params 业务参数（含测试开关），字段名与参与方端点请求体对齐
     */
    public TccRemoteResult tryInvoke(TccActionInfo action, String xid, Map<String, Object> params) {
        Map<String, Object> body = new HashMap<>(params == null ? new HashMap<String, Object>() : params);
        body.put("xid", xid);
        String url = action.getServiceUrl() + "/tcc/" + action.getActionName();
        try {
            ResponseEntity<TccRemoteResult> resp =
                    restTemplate.postForEntity(url, body, TccRemoteResult.class);
            TccRemoteResult r = resp.getBody();
            return r == null ? new TccRemoteResult(false, "UNKNOWN", "参与方返回空响应") : r;
        } catch (Exception e) {
            log.error("[Invoker] 直调动作 {} 一阶段失败：{}", action.getActionName(), e.getMessage());
            return new TccRemoteResult(false, "UNKNOWN",
                    "调用参与方失败（" + action.getServiceName() + "）：" + e.getMessage());
        }
    }

    /**
     * 二阶段：统一信封调用（参与方反射分发）。
     * @param phase  CONFIRM / CANCEL（TRY 供手工重放/防悬挂验证用）
     * @param params 业务参数快照（来自分支日志），按参数名与参与方方法形参绑定
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
