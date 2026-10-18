package com.demo.order.service;

import com.demo.order.tcc.TccTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 订单业务服务：纯业务 + 本地事务边界。
 *
 * placeOrder 用 @Transactional 控制本地事务——本地业务就是正常调用：
 * 不需要拆成 Try/Confirm/Cancel 三段，提交即生效、回滚即从未存在。
 * 远程分支由 TccTransactionManager 以【事务回调】的方式协调：
 * 本地事务提交后自动 Confirm、回滚后自动 Cancel，业务代码无感知。
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final TccTransactionManager tccManager;
    private final JdbcTemplate jdbcTemplate;

    public OrderService(TccTransactionManager tccManager, JdbcTemplate jdbcTemplate) {
        this.tccManager = tccManager;
        this.jdbcTemplate = jdbcTemplate;
    }

    /** 下单业务入口（本地事务由 @Transactional 控制） */
    @Transactional
    public Map<String, Object> placeOrder(long productId, int count,
                                          boolean mockFailTry, boolean mockFailConfirm, boolean mockFailCancel) {
        Map<String, Object> result = new LinkedHashMap<>();

        // 业务校验（事务机制之外的业务规则；不通过则空事务照常提交，无副作用）
        if (productId <= 0 || count < 1) {
            log.warn("[Order] 业务校验不通过：productId={}, count={}", productId, count);
            result.put("success", false);
            result.put("globalStatus", "NONE");
            result.put("message", "业务校验不通过：productId 必须大于 0 且 count 必须大于等于 1");
            return result;
        }

        String xid = UUID.randomUUID().toString();
        result.put("xid", xid);

        try {
            // ① 本地业务：创建订单（正常调用，随本地事务提交/回滚）
            createOrder(xid, productId, count);
            // ② 远程分支：Try + 注册二阶段事务回调（提交->Confirm / 回滚->Cancel）
            tccManager.registerRemoteBranch(xid, productId, count,
                    mockFailTry, mockFailConfirm, mockFailCancel, result);
        } catch (Exception e) {
            // 任何异常都不向外抛（抛出会成为 Controller 的 500）：
            // 标记回滚 -> 方法正常返回 -> Spring 执行回滚 -> 触发 Cancel 回调
            log.warn("[Order] 本地事务内出现异常，标记回滚：{}", e.getMessage());
            result.putIfAbsent("storageTryMessage", "Try 未发起或未成功：" + e.getMessage());
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }
        return result;
        // 方法返回后由 Spring 决定提交/回滚，并在事务代理层同步执行二阶段回调填好 result——
        // Controller 拿到的已是包含 Confirm/Cancel 最终结果的完整内容
    }

    /** 本地业务：创建订单（一条普通 INSERT，随本地事务提交/回滚，无中间状态） */
    private void createOrder(String xid, long productId, int count) {
        log.info("[Order] 本地业务：创建订单，商品 {} 数量 {}（xid={}）", productId, count, xid);
        jdbcTemplate.update(
                "INSERT INTO t_order(xid, product_id, order_count) VALUES (?, ?, ?)",
                xid, productId, count);
    }
}
