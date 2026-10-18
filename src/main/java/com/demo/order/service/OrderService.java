package com.demo.order.service;

import com.demo.order.tcc.TccTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 订单业务服务：纯业务入口，不包含任何 TCC 事务机制代码。
 *
 * 职责：业务校验、组装业务参数，然后把“下单”作为一个分布式事务
 * 委托给 TccTransactionManager 执行——何时 Try、何时 Confirm/Cancel、
 * 失败怎么重试、全局日志怎么记，都是事务处理器的能力，与本类无关。
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final TccTransactionManager tccManager;

    public OrderService(TccTransactionManager tccManager) {
        this.tccManager = tccManager;
    }

    /** 下单业务入口 */
    public Map<String, Object> placeOrder(long productId, int count,
                                          boolean mockFailTry, boolean mockFailConfirm, boolean mockFailCancel) {
        // 业务校验（事务机制之外的业务规则）
        if (productId <= 0 || count < 1) {
            log.warn("[Order] 业务校验不通过：productId={}, count={}", productId, count);
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("success", false);
            result.put("globalStatus", "NONE");
            result.put("message", "业务校验不通过：productId 必须大于 0 且 count 必须大于等于 1");
            return result;
        }
        // 委托 TCC 事务处理器执行全局事务
        return tccManager.placeOrder(productId, count, mockFailTry, mockFailConfirm, mockFailCancel);
    }
}
