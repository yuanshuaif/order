package com.demo.order.service;

import com.demo.order.client.TccRemoteResult;
import com.demo.order.tcc.TccPhaseResult;
import com.demo.order.tcc.TccTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 订单业务服务：全局事务的发起与编排方。
 *
 * 职责划分：
 *   - 本地订单分支：委托 OrderTccService，每个阶段是独立的本地事务，
 *     由 Spring 事务管理器（@Transactional）保证原子性，不需要重试；
 *   - 远程库存分支：委托 TccTransactionManager，仅负责远程调用的协调与失败重试；
 *   - 全局事务日志（t_global_txn_log）：由本服务记录，反应全局事务最终状态。
 *
 * 注意：placeOrder 本身不加 @Transactional —— TCC 的三个阶段必须各自独立提交，
 * 若整个方法套在一个本地事务里，Try 的资源预留就被锁在本地事务内，失去 TCC 意义。
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderTccService orderTccService;
    private final TccTransactionManager tccManager;
    private final JdbcTemplate jdbcTemplate;

    public OrderService(OrderTccService orderTccService,
                        TccTransactionManager tccManager,
                        JdbcTemplate jdbcTemplate) {
        this.orderTccService = orderTccService;
        this.tccManager = tccManager;
        this.jdbcTemplate = jdbcTemplate;
    }

    /** 下单业务入口：执行一次完整的 TCC 分布式事务 */
    public Map<String, Object> placeOrder(long productId, int count,
                                          boolean mockFailTry, boolean mockFailConfirm, boolean mockFailCancel) {
        long start = System.currentTimeMillis();
        String xid = UUID.randomUUID().toString();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("xid", xid);

        saveGlobalLog(xid, "TRYING", "事务开始");
        log.info("[TCC] ===== 全局事务开始 xid={} =====", xid);

        // ---------------- Try 阶段：资源预留（本地一次 + 远程一次） ----------------
        boolean orderTryOk = false;
        try {
            log.info("[TCC] Try 阶段 -> 订单分支(本地)：创建订单(状态 TRYING)，商品 {} 数量 {}", productId, count);
            orderTccService.tryCreateOrder(xid, productId, count);
            orderTryOk = true;
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Try 失败：{}", e.getMessage());
        }

        boolean storageTryOk = false;
        if (orderTryOk) {
            TccRemoteResult r = tccManager.tryDeduct(xid, productId, count, mockFailTry);
            storageTryOk = r.isSuccess();
            result.put("storageTryMessage", r.getMessage());
        }

        // ---------------- Try 失败 -> 全局回滚：本地 Cancel 一次 + 远程 Cancel 重试 ----------------
        if (!orderTryOk || !storageTryOk) {
            log.warn("[TCC] Try 阶段失败(orderTryOk={}, storageTryOk={})，执行全局 Cancel", orderTryOk, storageTryOk);
            boolean orderCancelOk = cancelLocal(xid);
            TccPhaseResult rc = tccManager.cancel(xid, mockFailCancel);

            if (orderCancelOk && rc.isSuccess()) {
                updateGlobalLog(xid, "CANCELLED", "Try失败，全局回滚成功（远程尝试 " + rc.getAttempts() + " 次）", rc.getAttempts());
                log.info("[TCC] 全局 Cancel 完成（远程尝试 {} 次）：订单=CANCELLED，库存分支={}", rc.getAttempts(), rc.getState());
                result.put("success", false);
                result.put("globalStatus", "CANCELLED");
                result.put("orderStatus", "CANCELLED");
                result.put("storageBranchState", rc.getState());
                result.put("retryCount", rc.getAttempts());
                result.put("message", "Try 阶段失败，已执行全局回滚（Cancel，尝试 " + rc.getAttempts() + " 次），库存解冻、订单取消");
            } else {
                updateGlobalLog(xid, "CANCEL_FAILED", "Cancel 重试 " + rc.getAttempts() + " 次仍失败，待后台恢复", rc.getAttempts());
                log.error("[TCC] Cancel 失败 xid={}（订单分支取消{}，远程尝试 {} 次），全局事务标记为 CANCEL_FAILED（预留资源未释放，等待恢复）",
                        xid, orderCancelOk ? "成功" : "失败", rc.getAttempts());
                result.put("success", false);
                result.put("globalStatus", "CANCEL_FAILED");
                result.put("orderStatus", orderCancelOk ? "CANCELLED" : "TRYING");
                result.put("storageBranchState", rc.getState());
                result.put("retryCount", rc.getAttempts());
                result.put("message", "Cancel 阶段失败（已重试 " + rc.getAttempts() + " 次，最后原因：" + rc.getMessage()
                        + "），预留资源未释放，等待后台恢复重试");
            }
            result.put("durationMs", System.currentTimeMillis() - start);
            return result;
        }

        // ---------------- Confirm 阶段：本地 Confirm 一次 + 远程 Confirm 重试 ----------------
        boolean orderConfirmOk = false;
        try {
            orderTccService.confirmOrder(xid);
            orderConfirmOk = true;
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Confirm 失败：{}", e.getMessage());
        }
        TccPhaseResult rc = tccManager.confirm(xid, mockFailConfirm);

        if (orderConfirmOk && rc.isSuccess()) {
            updateGlobalLog(xid, "CONFIRMED", "两分支 Confirm 成功（远程尝试 " + rc.getAttempts() + " 次）", rc.getAttempts());
            log.info("[TCC] ===== 全局事务提交成功 xid={} =====", xid);
            result.put("success", true);
            result.put("globalStatus", "CONFIRMED");
            result.put("orderStatus", "CONFIRMED");
            result.put("storageBranchState", "CONFIRMED");
            result.put("retryCount", rc.getAttempts());
            result.put("message", "下单成功：订单已生效，库存冻结量已转为实际扣减");
        } else {
            // Confirm 一旦开始就不允许回滚，重试耗尽只能留待后台恢复
            updateGlobalLog(xid, "CONFIRM_FAILED", "Confirm 重试 " + rc.getAttempts() + " 次仍失败，待后台恢复", rc.getAttempts());
            log.error("[TCC] Confirm 失败 xid={}（订单分支确认{}，远程尝试 {} 次），全局事务标记为 CONFIRM_FAILED（等待恢复，不回滚）",
                    xid, orderConfirmOk ? "成功" : "失败", rc.getAttempts());
            result.put("success", false);
            result.put("globalStatus", "CONFIRM_FAILED");
            result.put("orderStatus", orderConfirmOk ? "CONFIRMED" : "TRYING");
            result.put("storageBranchState", rc.getState());
            result.put("retryCount", rc.getAttempts());
            result.put("message", "Confirm 阶段失败（已重试 " + rc.getAttempts() + " 次），按 TCC 语义不回滚，等待后续恢复重试");
        }
        result.put("durationMs", System.currentTimeMillis() - start);
        return result;
    }

    private boolean cancelLocal(String xid) {
        try {
            orderTccService.cancelOrder(xid);
            return true;
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Cancel 失败：{}", e.getMessage());
            return false;
        }
    }

    private void saveGlobalLog(String xid, String status, String detail) {
        jdbcTemplate.update("INSERT INTO t_global_txn_log(xid, status, detail) VALUES (?, ?, ?)", xid, status, detail);
    }

    private void updateGlobalLog(String xid, String status, String detail, int retryCount) {
        jdbcTemplate.update("UPDATE t_global_txn_log SET status = ?, detail = ?, retry_count = ? WHERE xid = ?",
                status, detail, retryCount, xid);
    }
}
