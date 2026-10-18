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
 *      （TCC 语义：一旦进入 Confirm，必须最终成功，不会回滚）
 */
@Component
public class TccTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(TccTransactionManager.class);

    /** Confirm 阶段失败重试次数（真实框架会由后台任务持续重试） */
    private static final int CONFIRM_MAX_RETRY = 3;
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
    public Map<String, Object> placeOrder(long productId, int count, boolean mockFailTry, boolean mockFailConfirm) {
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

        // ---------------- Try 失败 -> 全局回滚（Cancel 所有分支） ----------------
        if (!orderTryOk || !storageTryOk) {
            log.warn("[TCC] Try 阶段失败(orderTryOk={}, storageTryOk={})，执行全局 Cancel", orderTryOk, storageTryOk);
            // 订单分支取消：幂等，即使 Try 未成功（UPDATE 影响 0 行）也安全
            cancelLocalBranch(xid);
            // 库存分支取消：若库存 Try 未实际执行，storage 侧会走“空回滚”逻辑并落标记
            TccRemoteResult cancelResult = storageClient.cancel(xid);
            log.info("[TCC] 全局 Cancel 完成：订单分支=CANCELLED，库存分支状态={}", cancelResult.getState());

            updateGlobalLog(xid, "CANCELLED", "Try失败，全局回滚");
            result.put("success", false);
            result.put("globalStatus", "CANCELLED");
            result.put("orderStatus", "CANCELLED");
            result.put("storageBranchState", cancelResult.getState());
            result.put("message", "Try 阶段失败，已执行全局回滚（Cancel），库存解冻、订单取消");
            result.put("durationMs", System.currentTimeMillis() - start);
            return result;
        }

        // ---------------- Confirm 阶段：提交（失败重试，不回滚） ----------------
        for (int i = 1; i <= CONFIRM_MAX_RETRY; i++) {
            boolean orderConfirmOk = confirmLocalBranch(xid);
            TccRemoteResult rc = storageClient.confirm(xid, mockFailConfirm);
            if (orderConfirmOk && rc.isSuccess()) {
                updateGlobalLog(xid, "CONFIRMED", "两分支 Confirm 成功");
                log.info("[TCC] ===== 全局事务提交成功 xid={} =====", xid);
                result.put("success", true);
                result.put("globalStatus", "CONFIRMED");
                result.put("orderStatus", "CONFIRMED");
                result.put("storageBranchState", "CONFIRMED");
                result.put("message", "下单成功：订单已生效，库存冻结量已转为实际扣减");
                result.put("durationMs", System.currentTimeMillis() - start);
                return result;
            }
            log.warn("[TCC] Confirm 第 {} 次失败(orderConfirmOk={}, storageConfirm={})，TCC 不回滚，准备重试",
                    i, orderConfirmOk, rc.getMessage());
            sleep(RETRY_INTERVAL_MS);
        }

        // 重试耗尽：真实框架中会留下事务日志由后台任务继续重试，直到 Confirm 最终成功
        updateGlobalLog(xid, "CONFIRM_FAILED", "Confirm 重试 " + CONFIRM_MAX_RETRY + " 次仍失败，待后台恢复");
        log.error("[TCC] Confirm 重试 {} 次仍失败 xid={}，全局事务标记为 CONFIRM_FAILED（等待恢复，不回滚）", CONFIRM_MAX_RETRY, xid);
        result.put("success", false);
        result.put("globalStatus", "CONFIRM_FAILED");
        result.put("orderStatus", "CONFIRMED");
        result.put("storageBranchState", "TRIED");
        result.put("message", "Confirm 阶段失败（已重试 " + CONFIRM_MAX_RETRY + " 次），按 TCC 语义不回滚，等待后续恢复重试");
        result.put("durationMs", System.currentTimeMillis() - start);
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

    private void cancelLocalBranch(String xid) {
        try {
            orderTccService.cancelOrder(xid);
        } catch (Exception e) {
            log.warn("[TCC] 订单分支 Cancel 失败：{}", e.getMessage());
        }
    }

    private void saveGlobalLog(String xid, String status, String detail) {
        jdbcTemplate.update("INSERT INTO t_global_txn_log(xid, status, detail) VALUES (?, ?, ?)", xid, status, detail);
    }

    private void updateGlobalLog(String xid, String status, String detail) {
        jdbcTemplate.update("UPDATE t_global_txn_log SET status = ?, detail = ? WHERE xid = ?", status, detail, xid);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
