package com.demo.order.tcc.remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 注册服务客户端（发起方 -> tcc-registry-service）
 *
 * 查询动作信息（actionName -> 服务地址与方法），带内存缓存：
 * 缓存为空、超过 TTL 或未命中时刷新；未命中时强制刷新一次，
 * 以便参与方"刚注册"就能被发现（demo 无心跳，靠查询兜底）。
 */
@Component
public class TccRegistryClient {

    private static final Logger log = LoggerFactory.getLogger(TccRegistryClient.class);

    /** 缓存有效期：生产中由心跳/推送保持新鲜 */
    private static final long CACHE_TTL_MS = 30_000L;

    private final RestTemplate restTemplate;
    private final String registryBaseUrl;

    private volatile Map<String, TccActionInfo> cache = Collections.emptyMap();
    private volatile long lastLoadAt = 0;

    public TccRegistryClient(@Value("${tcc.registry.base-url}") String registryBaseUrl) {
        this.registryBaseUrl = registryBaseUrl;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(3000);
        factory.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(factory);
    }

    /** 按动作名查询；查不到抛异常（动作名写错时快速失败） */
    public TccActionInfo lookup(String actionName) {
        TccActionInfo info = lookupQuiet(actionName);
        if (info == null) {
            throw new IllegalStateException("注册服务中不存在 TCC 动作：" + actionName
                    + "（参与方未注册或注册服务不可达）");
        }
        return info;
    }

    /** 查不到返回 null（二阶段回调里用：注册服务临时不可用时按分支失败处理，而非中断） */
    public TccActionInfo lookupQuiet(String actionName) {
        long now = System.currentTimeMillis();
        if (cache.isEmpty() || now - lastLoadAt > CACHE_TTL_MS) {
            refresh();
        }
        TccActionInfo info = cache.get(actionName);
        if (info == null) {
            // 未命中强制刷新一次：参与方可能刚刚才注册上
            refresh();
            info = cache.get(actionName);
        }
        return info;
    }

    private synchronized void refresh() {
        try {
            TccActionInfo[] actions = restTemplate.getForObject(
                    registryBaseUrl + "/tcc/actions", TccActionInfo[].class);
            Map<String, TccActionInfo> fresh = new HashMap<>();
            if (actions != null) {
                for (TccActionInfo a : actions) {
                    fresh.put(a.getActionName(), a);
                }
            }
            cache = fresh;
            lastLoadAt = System.currentTimeMillis();
            log.debug("[RegistryClient] 刷新注册表，共 {} 个动作", fresh.size());
        } catch (Exception e) {
            log.warn("[RegistryClient] 刷新注册表失败（注册服务不可达？沿用旧缓存）：{}", e.getMessage());
            lastLoadAt = System.currentTimeMillis();
        }
    }
}
