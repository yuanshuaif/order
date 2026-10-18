package com.demo.order.tcc;

import com.demo.order.client.StorageClient;
import com.demo.order.client.TccRemoteResult;
import com.demo.order.service.OrderTccService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * TCC 事务处理器（全局事务协调器，等价 Seata 中 TM + TC 的简化版）
 *
 * 能力边界——只关心“事务”，不关心“业务”：
 *   - 编排全局事务生命周期：Try -> Confirm（提交）或 Cancel（回滚）
 *   - 本地分支：委托 OrderTccService，各阶段独立本地事务，
 *     由 Spring 事务管理器控制原子性，本地调用无网络不确定性、不需要重试
 *   - 远程分支：通过 StorageClient 调用，Confirm/Cancel 失败重试到成功为止
 *   - 全局事务日志（t_global_txn_log）：xid、状态机、重试次数、终态
 *
 * 二阶段重试语义（与 Seata TC 一致）：
 *   - Confirm 失败：重试，不回滚（Try 都已成功，回滚反而制造不一致）
 *   - Cancel  失败：重试，不放弃（Try 已成功分支的预留资源必须被释放）
 *   - 重试耗尽：置 CONFIRM_FAILED / CANCEL_FAILED 终态，留待后台恢复任务
 */
@Component
public class TccTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(TccTransactionManager.class);

    /** Confirm 阶段失败重试次数（真实框架会由后台任务持续重试） */
    private static final int CONFIRM_MAX_RETRY = 3;
    /** Cancel 阶段失败重试次数（与 Confirm 同等对待，回滚也必须最终成功） */
    private static final int CANCEL_MAX_RETRY = 3;
    private static final long RETRY_INTERVAL_MS = 500L;

    /** 本地订单分支（RM）：业务与本地事务都在它那边，这里只调用 */
    private final OrderTccService orderTccService;
    /** 远程库存分支（RM 代理） */
    private final StorageClient storageClient;
    /** 全局事务日志 */
    private final JdbcTemplate jdbcTemplate;

    public TccTransactionManager(OrderTccService orderTccService,
                                 StorageClient storageClient,
                                 JdbcTemplate jdbcTemplate) {
        this.orderTccService = orderTccService;
        this.storageClient = storageClient;
        this.jdbcTemplate = jdbcTemplate;
    }

    /** 执行一次完整的 TCC 全局事务（下单） */
    public Map<String, Object> placeOrder(long productId, int count,
                                          boolean mockFailTry, boolean mockFailConfirm, boolean mockFailCancel) {
        long start = System.currentTimeMillis();
        String xid = UUID.randomUUID().toString();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("xid", xid);

        saveGlobalLog(xid, "TRYING", "事务开始");
        log.info("[TCC] ===== 全局事务开始 xid={} =====", xid);

        // ---------------- Try 阶段：资源预留（本地一次 + 远程一次） ----------------
        boolean orderTryOk = tryLocalBranch(xid, productId, count);
        boolean storageTryOk = false;
        if (orderTryOk) {
            log.info("[TCC] Try 阶段 -> 库存分支(远程)：冻结商品 {} 库存 {} 件", productId, count);
            TccRemoteResult r = storageClient.tryDeduct(xid, productId, count, mockFailTry);
            storageTryOk = r.isSuccess();
            result.put("storageTryMessage", r.getMessage());
        }

        // ---------------- Try 失败 -> 全局回滚：本地 Cancel 一次 + 远程 Cancel 重试 ----------------
        if (!orderTryOk || !storageTryOk) {
            log.warn("[TCC] Try 阶段失败(orderTryOk={}, storageTryOk={})，执行全局 Cancel", orderTryOk, storageTryOk);
            boolean orderCancelOk = cancelLocalBranch(xid);
            TccPhaseResult rc = cancelRemoteWithRetry(xid, mockFailCancel);

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
        boolean orderConfirmOk = confirmLocalBranch(xid);
        TccPhaseResult rc = confirmRemoteWithRetry(xid, mockFailConfirm);

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

    // ---------------- 本地分支：单次调用，Spring 事务管理器保证原子性，不重试 ----------------

    private boolean tryLocalBranch(String xid, long productId, int count) {
        try {
            log.info("[TCC] Try 阶段 -> 订单分支(本地)：创建订单(状态 TRYING)，商品 {} 数量 {}", productId, count);
            orderTccService.tryCreateOrder(xid, productId, count);
            return true;
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Try 失败：{}", e.getMessage());
            return false;
        }
    }

    private boolean confirmLocalBranch(String xid) {
        try {
            orderTccService.confirmOrder(xid);
            return true;
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Confirm 失败：{}", e.getMessage());
            return false;
        }
    }

    private boolean cancelLocalBranch(String xid) {
        try {
            orderTccService.cancelOrder(xid);
            return true;
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Cancel 失败：{}", e.getMessage());
            return false;
        }
    }

    // ---------------- 远程分支：二阶段失败重试 ----------------

    /** 远程库存分支 Confirm：失败重试，不回滚 */
    private TccPhaseResult confirmRemoteWithRetry(String xid, boolean mockFailConfirm) {
        TccRemoteResult r = null;
        int attempts = 0;
        for (int i = 1; i <= CONFIRM_MAX_RETRY; i++) {
            attempts = i;
            r = storageClient.confirm(xid, mockFailConfirm);
            if (r.isSuccess()) {
                return new TccPhaseResult(true, r.getState(), r.getMessage(), attempts);
            }
            log.warn("[TCC] 远程 Confirm 第 {} 次失败({})，TCC 不回滚，准备重试", i, r.getMessage());
            sleep(RETRY_INTERVAL_MS);
        }
        return new TccPhaseResult(false, r.getState(), r.getMessage(), attempts);
    }

    /** 远程库存分支 Cancel：失败重试，不放弃 */
    private TccPhaseResult cancelRemoteWithRetry(String xid, boolean mockFailCancel) {
        TccRemoteResult r = null;
        int attempts = 0;
        for (int i = 1; i <= CANCEL_MAX_RETRY; i++) {
            attempts = i;
            r = storageClient.cancel(xid, mockFailCancel);
            if (r.isSuccess()) {
                return new TccPhaseResult(true, r.getState(), r.getMessage(), attempts);
            }
            log.warn("[TCC] 远程 Cancel 第 {} 次失败({})，回滚必须成功，准备重试", i, r.getMessage());
            sleep(RETRY_INTERVAL_MS);
        }
        return new TccPhaseResult(false, r.getState(), r.getMessage(), attempts);
    }

    // ---------------- 全局事务日志 ----------------

    private void saveGlobalLog(String xid, String status, String detail) {
        jdbcTemplate.update("INSERT INTO t_global_txn_log(xid, status, detail) VALUES (?, ?, ?)", xid, status, detail);
    }

    private void updateGlobalLog(String xid, String status, String detail, int retryCount) {
        jdbcTemplate.update("UPDATE t_global_txn_log SET status = ?, detail = ?, retry_count = ? WHERE xid = ?",
                status, detail, retryCount, xid);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
