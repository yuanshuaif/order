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
 * 手写 TCC 事务协调器（demo 版，运行在订单服务内，等价于 Seata 中 TM + TC 的角色）
 *
 * 全局事务包含两个分支：
 *   分支1（本地）：订单分支  OrderTccService
 *   分支2（远程）：库存分支  storage-service 的 /storage/try|confirm|cancel
 *
 * 协调流程：
 *   1. 生成全局事务 id xid，全局日志置 TRYING
 *   2. Try 阶段：订单Try(建订单置TRYING) -> 库存Try(冻结库存)；任一失败 -> 全局 Cancel
 *   3. Confirm 阶段：订单Confirm(订单生效) + 库存Confirm(冻结转实扣)，失败重试，不回滚
 *   4. Cancel  阶段：订单Cancel + 库存Cancel(解冻)，失败同样重试
 *
 * 二阶段（Confirm/Cancel）的重试语义：
 *   - 重试次数记录在全局事务日志 t_global_txn_log.retry_count
 *   - 重试耗尽仍失败 -> 置 CONFIRM_FAILED / CANCEL_FAILED 终态，
 *     留待后台恢复任务继续重试（真实框架如 Seata 由 TC 自动完成）
 */
@Component
public class TccTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(TccTransactionManager.class);

    /** Confirm 阶段失败重试次数（真实框架会由后台任务持续重试） */
    private static final int CONFIRM_MAX_RETRY = 3;
    /** Cancel 阶段失败重试次数（与 Confirm 同等对待，回滚也必须最终成功） */
    private static final int CANCEL_MAX_RETRY = 3;
    private static final long RETRY_INTERVAL_MS = 500L;

    private final OrderTccService orderTccService;
    private final StorageClient storageClient;
    private final JdbcTemplate jdbcTemplate;

    public TccTransactionManager(OrderTccService orderTccService,
                                 StorageClient storageClient,
                                 JdbcTemplate jdbcTemplate) {
        this.orderTccService = orderTccService;
        this.storageClient = storageClient;
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

        // ---------------- Try 阶段：资源预留 ----------------
        boolean orderTryOk = tryLocalBranch(xid, productId, count);
        boolean storageTryOk = false;
        if (orderTryOk) {
            log.info("[TCC] Try 阶段 -> 库存分支：冻结商品 {} 库存 {} 件", productId, count);
            TccRemoteResult r = storageClient.tryDeduct(xid, productId, count, mockFailTry);
            storageTryOk = r.isSuccess();
            result.put("storageTryMessage", r.getMessage());
        }

        // ---------------- Try 失败 -> 全局回滚（Cancel，失败重试） ----------------
        if (!orderTryOk || !storageTryOk) {
            log.warn("[TCC] Try 阶段失败(orderTryOk={}, storageTryOk={})，执行全局 Cancel", orderTryOk, storageTryOk);
            Map<String, Object> cancelResult = cancelAllWithRetry(xid, mockFailCancel);
            result.putAll(cancelResult);
            result.put("durationMs", System.currentTimeMillis() - start);
            return result;
        }

        // ---------------- Confirm 阶段：提交（失败重试，不回滚） ----------------
        for (int i = 1; i <= CONFIRM_MAX_RETRY; i++) {
            boolean orderConfirmOk = confirmLocalBranch(xid);
            TccRemoteResult rc = storageClient.confirm(xid, mockFailConfirm);
            if (orderConfirmOk && rc.isSuccess()) {
                updateGlobalLog(xid, "CONFIRMED", "两分支 Confirm 成功（尝试 " + i + " 次）", i);
                log.info("[TCC] ===== 全局事务提交成功 xid={} =====", xid);
                result.put("success", true);
                result.put("globalStatus", "CONFIRMED");
                result.put("orderStatus", "CONFIRMED");
                result.put("storageBranchState", "CONFIRMED");
                result.put("retryCount", i);
                result.put("message", "下单成功：订单已生效，库存冻结量已转为实际扣减");
                result.put("durationMs", System.currentTimeMillis() - start);
                return result;
            }
            log.warn("[TCC] Confirm 第 {} 次失败(orderConfirmOk={}, storageConfirm={})，TCC 不回滚，准备重试",
                    i, orderConfirmOk, rc.getMessage());
            updateGlobalLog(xid, "TRYING", "Confirm 第 " + i + " 次失败，等待重试", i);
            sleep(RETRY_INTERVAL_MS);
        }

        // 重试耗尽：真实框架中会留下事务日志由后台任务继续重试，直到 Confirm 最终成功
        updateGlobalLog(xid, "CONFIRM_FAILED", "Confirm 重试 " + CONFIRM_MAX_RETRY + " 次仍失败，待后台恢复", CONFIRM_MAX_RETRY);
        log.error("[TCC] Confirm 重试 {} 次仍失败 xid={}，全局事务标记为 CONFIRM_FAILED（等待恢复，不回滚）", CONFIRM_MAX_RETRY, xid);
        result.put("success", false);
        result.put("globalStatus", "CONFIRM_FAILED");
        result.put("orderStatus", "CONFIRMED");
        result.put("storageBranchState", "TRIED");
        result.put("retryCount", CONFIRM_MAX_RETRY);
        result.put("message", "Confirm 阶段失败（已重试 " + CONFIRM_MAX_RETRY + " 次），按 TCC 语义不回滚，等待后续恢复重试");
        result.put("durationMs", System.currentTimeMillis() - start);
        return result;
    }

    /**
     * 全局 Cancel：取消所有分支，失败重试，重试耗尽置 CANCEL_FAILED。
     * Cancel 与 Confirm 同等重要——Try 已成功的分支若 Cancel 失败，预留资源将无法释放，
     * 因此回滚同样必须最终成功。
     */
    private Map<String, Object> cancelAllWithRetry(String xid, boolean mockFailCancel) {
        Map<String, Object> result = new LinkedHashMap<>();
        boolean orderCancelOk = false;
        boolean storageCancelOk = false;
        String storageCancelState = "NONE";
        String lastMessage = "";
        int attempts = 0;

        for (int i = 1; i <= CANCEL_MAX_RETRY; i++) {
            attempts = i;
            orderCancelOk = cancelLocalBranch(xid);
            TccRemoteResult rc = storageClient.cancel(xid, mockFailCancel);
            storageCancelOk = rc.isSuccess();
            storageCancelState = rc.getState();
            lastMessage = rc.getMessage();
            if (orderCancelOk && storageCancelOk) {
                break;
            }
            log.warn("[TCC] Cancel 第 {} 次失败(orderCancelOk={}, storageCancel={})，回滚必须成功，准备重试",
                    i, orderCancelOk, lastMessage);
            updateGlobalLog(xid, "TRYING", "Cancel 第 " + i + " 次失败，等待重试", i);
            sleep(RETRY_INTERVAL_MS);
        }

        if (orderCancelOk && storageCancelOk) {
            updateGlobalLog(xid, "CANCELLED", "Try失败，全局回滚成功（尝试 " + attempts + " 次）", attempts);
            log.info("[TCC] 全局 Cancel 完成（尝试 {} 次）：订单分支=CANCELLED，库存分支状态={}", attempts, storageCancelState);
            result.put("success", false);
            result.put("globalStatus", "CANCELLED");
            result.put("orderStatus", orderCancelOk ? "CANCELLED" : "TRYING");
            result.put("storageBranchState", storageCancelState);
            result.put("retryCount", attempts);
            result.put("message", "Try 阶段失败，已执行全局回滚（Cancel，尝试 " + attempts + " 次），库存解冻、订单取消");
        } else {
            updateGlobalLog(xid, "CANCEL_FAILED", "Cancel 重试 " + CANCEL_MAX_RETRY + " 次仍失败，待后台恢复", CANCEL_MAX_RETRY);
            log.error("[TCC] Cancel 重试 {} 次仍失败 xid={}，全局事务标记为 CANCEL_FAILED（预留资源未释放，等待恢复）",
                    CANCEL_MAX_RETRY, xid);
            result.put("success", false);
            result.put("globalStatus", "CANCEL_FAILED");
            result.put("orderStatus", orderCancelOk ? "CANCELLED" : "TRYING");
            result.put("storageBranchState", storageCancelState);
            result.put("retryCount", CANCEL_MAX_RETRY);
            result.put("message", "Cancel 阶段失败（已重试 " + CANCEL_MAX_RETRY + " 次，最后原因：" + lastMessage
                    + "），预留资源未释放，等待后台恢复重试");
        }
        return result;
    }

    private boolean tryLocalBranch(String xid, long productId, int count) {
        try {
            log.info("[TCC] Try 阶段 -> 订单分支：创建订单(状态 TRYING)，商品 {} 数量 {}", productId, count);
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
