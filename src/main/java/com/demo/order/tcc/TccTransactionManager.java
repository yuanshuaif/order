package com.demo.order.tcc;

import com.demo.order.client.StorageClient;
import com.demo.order.client.TccRemoteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;

/**
 * TCC 事务处理器——只控制远程分支，本地业务完全不由它经手
 *
 * 在业务方法的 @Transactional 本地事务内调用 registerRemoteBranch(...)：
 *   1. 写全局日志 TRYING（随本地事务提交/回滚，保证“业务数据 + 协调意图”原子落库）
 *   2. 注册事务回调（TransactionSynchronization）：
 *        afterCommit            -> 远程 Confirm（失败重试，不回滚）
 *        afterCompletion(回滚)   -> 远程 Cancel（失败重试，不放弃）
 *   3. 调用远程 Try；失败不抛异常，只把本地事务标记为 rollbackOnly——
 *      业务方法正常返回后由 Spring 执行回滚，进而触发 Cancel 回调。
 *      （不能在这里抛异常：会一路传到 Controller 变成 500，拿不到结构化结果；
 *        也不能在业务方法里 catch 吞掉：那会变成正常返回、误提交）
 *
 * 回调里写全局日志必须用独立事务（REQUIRES_NEW）：回调执行时本地事务已提交/回滚，
 * 但连接可能仍绑定在线程上，直接写会“搭”在已结束的事务上（Spring 文档明确要求
 * 回调中的数据访问声明 REQUIRES_NEW）。
 *
 * 二阶段重试语义（与 Seata TC 一致）：
 *   - Confirm 失败：重试，不回滚（Try 已成功、本地事务已提交，回滚反而制造不一致）
 *   - Cancel  失败：重试，不放弃（Try 已落地的分支其预留资源必须被释放）
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

    /** 远程库存分支（RM 代理） */
    private final StorageClient storageClient;
    /** 全局事务日志 */
    private final JdbcTemplate jdbcTemplate;
    /** 二阶段回调里写全局日志用的独立事务模板（REQUIRES_NEW） */
    private final TransactionTemplate logTxTemplate;

    public TccTransactionManager(StorageClient storageClient,
                                 JdbcTemplate jdbcTemplate,
                                 PlatformTransactionManager transactionManager) {
        this.storageClient = storageClient;
        this.jdbcTemplate = jdbcTemplate;
        this.logTxTemplate = new TransactionTemplate(transactionManager,
                new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    }

    /**
     * 注册远程库存分支——必须在业务方法的 @Transactional 本地事务内调用。
     * 之后远程分支的二阶段完全由本地事务的结局驱动，业务方法无需再做任何事。
     *
     * @param result 业务方法正在组装的返回结果（回调执行完毕后才是完整内容；
     *               回调在事务代理层同步执行，Controller 拿到它时已填好）
     */
    public void registerRemoteBranch(String xid, long productId, int count,
                                     boolean mockFailTry, boolean mockFailConfirm, boolean mockFailCancel,
                                     Map<String, Object> result) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("registerRemoteBranch 必须在 @Transactional 本地事务内调用");
        }
        long start = System.currentTimeMillis();
        log.info("[TCC] ===== 全局事务开始 xid={} =====", xid);

        // 协调意图，随本地事务提交/回滚（回滚则此行消失，Cancel 回调再补记终态）
        saveGlobalLog(xid, "TRYING", "事务开始", 0);

        // 先注册回调、后发起远程调用：保证远程 Try 只要发起过，Cancel 回调必然存在
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                confirmPhase(xid, mockFailConfirm, result, start);
            }

            @Override
            public void afterCompletion(int status) {
                if (status == TransactionSynchronization.STATUS_ROLLED_BACK) {
                    cancelPhase(xid, mockFailCancel, result, start);
                }
                // STATUS_COMMITTED 时 afterCommit 已处理，跳过
            }
        });

        log.info("[TCC] Try -> 库存分支(远程)：冻结商品 {} 库存 {} 件", productId, count);
        TccRemoteResult r = storageClient.tryDeduct(xid, productId, count, mockFailTry);
        result.put("storageTryMessage", r.getMessage());

        if (!r.isSuccess()) {
            log.warn("[TCC] 远程 Try 失败({})，标记本地事务回滚（rollbackOnly）", r.getMessage());
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }
    }

    // ---------------- 二阶段回调 ----------------

    /** 本地事务已提交 -> 远程 Confirm（失败重试，不回滚） */
    private void confirmPhase(String xid, boolean mockFailConfirm, Map<String, Object> result, long start) {
        try {
            log.info("[TCC] 本地事务已提交 -> 远程 Confirm xid={}", xid);
            TccPhaseResult rc = confirmRemoteWithRetry(xid, mockFailConfirm);

            if (rc.isSuccess()) {
                logTxTemplate.executeWithoutResult(s -> updateGlobalLog(xid, "CONFIRMED",
                        "本地事务已提交，远程 Confirm 成功（尝试 " + rc.getAttempts() + " 次）", rc.getAttempts()));
                log.info("[TCC] ===== 全局事务提交成功 xid={} =====", xid);
                result.put("success", true);
                result.put("globalStatus", "CONFIRMED");
                result.put("orderStatus", "CREATED");
                result.put("storageBranchState", rc.getState());
                result.put("retryCount", rc.getAttempts());
                result.put("message", "下单成功：本地事务已提交（订单生效），远程库存冻结量已转为实际扣减");
            } else {
                // Confirm 一旦开始就不允许回滚，重试耗尽只能留待后台恢复
                logTxTemplate.executeWithoutResult(s -> updateGlobalLog(xid, "CONFIRM_FAILED",
                        "Confirm 重试 " + rc.getAttempts() + " 次仍失败，待后台恢复", rc.getAttempts()));
                log.error("[TCC] Confirm 失败 xid={}（远程尝试 {} 次），全局事务标记为 CONFIRM_FAILED（等待恢复，不回滚）", xid, rc.getAttempts());
                result.put("success", false);
                result.put("globalStatus", "CONFIRM_FAILED");
                result.put("orderStatus", "CREATED");
                result.put("storageBranchState", rc.getState());
                result.put("retryCount", rc.getAttempts());
                result.put("message", "本地事务已提交，但远程 Confirm 失败（已重试 " + rc.getAttempts()
                        + " 次），按 TCC 语义不回滚，等待后续恢复重试");
            }
            result.put("durationMs", System.currentTimeMillis() - start);
        } catch (Exception e) {
            // 回调绝不能向外抛（事务已提交，抛出只会搅乱调用方），记日志留待人工/后台处理
            log.error("[TCC] Confirm 回调异常 xid={}：{}", xid, e.getMessage(), e);
        }
    }

    /** 本地事务已回滚 -> 远程 Cancel（失败重试，不放弃） */
    private void cancelPhase(String xid, boolean mockFailCancel, Map<String, Object> result, long start) {
        try {
            log.warn("[TCC] 本地事务已回滚 -> 远程 Cancel xid={}", xid);
            result.putIfAbsent("storageTryMessage", "Try 未发起或未成功");
            TccPhaseResult rc = cancelRemoteWithRetry(xid, mockFailCancel);

            if (rc.isSuccess()) {
                // TRYING 行已随回滚消失，在独立事务里补记终态
                logTxTemplate.executeWithoutResult(s -> saveGlobalLog(xid, "CANCELLED",
                        "本地事务回滚，远程 Cancel 成功（尝试 " + rc.getAttempts() + " 次）", rc.getAttempts()));
                log.info("[TCC] 全局 Cancel 完成（远程尝试 {} 次）：订单未落库，库存分支={}", rc.getAttempts(), rc.getState());
                result.put("success", false);
                result.put("globalStatus", "CANCELLED");
                result.put("orderStatus", "NONE");
                result.put("storageBranchState", rc.getState());
                result.put("retryCount", rc.getAttempts());
                result.put("message", "本地事务已回滚（远程 Cancel 尝试 " + rc.getAttempts()
                        + " 次）：订单未落库，库存已解冻或无变化");
            } else {
                logTxTemplate.executeWithoutResult(s -> saveGlobalLog(xid, "CANCEL_FAILED",
                        "Cancel 重试 " + rc.getAttempts() + " 次仍失败，待后台恢复", rc.getAttempts()));
                log.error("[TCC] Cancel 失败 xid={}（远程尝试 {} 次），全局事务标记为 CANCEL_FAILED（预留资源未释放，等待恢复）", xid, rc.getAttempts());
                result.put("success", false);
                result.put("globalStatus", "CANCEL_FAILED");
                result.put("orderStatus", "NONE");
                result.put("storageBranchState", rc.getState());
                result.put("retryCount", rc.getAttempts());
                result.put("message", "本地事务已回滚，但远程 Cancel 失败（已重试 " + rc.getAttempts() + " 次，最后原因："
                        + rc.getMessage() + "），预留资源未释放，等待后台恢复重试");
            }
            result.put("durationMs", System.currentTimeMillis() - start);
        } catch (Exception e) {
            // 回调绝不能向外抛（事务已回滚，抛出只会搅乱调用方），记日志留待人工/后台处理
            log.error("[TCC] Cancel 回调异常 xid={}：{}", xid, e.getMessage(), e);
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

    private void saveGlobalLog(String xid, String status, String detail, int retryCount) {
        jdbcTemplate.update("INSERT INTO t_global_txn_log(xid, status, detail, retry_count) VALUES (?, ?, ?, ?)",
                xid, status, detail, retryCount);
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
