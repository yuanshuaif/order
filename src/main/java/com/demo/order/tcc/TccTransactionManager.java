package com.demo.order.tcc;

import com.demo.order.client.TccRemoteResult;
import com.demo.order.tcc.remote.RemoteBranchInvoker;
import com.demo.order.tcc.remote.TccActionInfo;
import com.demo.order.tcc.remote.TccRegistryClient;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TCC 事务处理器——通用协调器，只控制远程分支，不经手本地业务
 *
 * 面向"动作"而非具体服务：业务方法在 @Transactional 本地事务内调
 * registerBranch(xid, actionName, params)，动作信息从注册服务查得，任何参与方通用。
 *
 *   1. 分支落库（t_global_branch_log：xid+actionName+方法快照+参数快照）——独立事务、先于 Try：
 *      分支日志记录的是"远程副作用已发生"的事实，必须活过本地回滚（否则回滚后查不到分支，
 *      Cancel 发不出去，已冻结的资源永远无人释放）；先于 Try 落库则 Try 中途崩溃也有 Cancel 兜底
 *   2. 全局 TRYING 日志随本地业务同一事务写入（提交才有记录）；首个分支注册一次事务回调：
 *        afterCommit           -> 逆序遍历该 xid 全部分支，逐个 Confirm（重试，不回滚）
 *        afterCompletion(回滚)  -> 逆序遍历全部分支，逐个 Cancel（重试，不放弃）
 *   3. 远程 Try：统一信封调 {serviceUrl}/tcc/invoke；失败不抛异常，只标记 rollbackOnly
 *
 * 回调里写库必须用独立事务（REQUIRES_NEW）：回调执行时本地事务已提交/回滚，
 * 但连接可能仍绑定在线程上，直接写会“搭”在已结束的事务上（Spring 文档要求）。
 *
 * 二阶段重试语义（与 Seata TC 一致）：
 *   - Confirm 失败：重试，不回滚（Try 已成功、本地事务已提交，回滚反而制造不一致）
 *   - Cancel  失败：重试，不放弃（Try 已落地的分支其预留资源必须被释放）
 *   - 重试耗尽：分支置 CONFIRM_FAILED / CANCEL_FAILED 终态，留待后台恢复任务
 */
@Component
public class TccTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(TccTransactionManager.class);

    /** 每个远程分支二阶段失败重试次数（真实框架会由后台任务持续重试） */
    private static final int CONFIRM_MAX_RETRY = 3;
    private static final int CANCEL_MAX_RETRY = 3;
    private static final long RETRY_INTERVAL_MS = 500L;

    private final TccRegistryClient registryClient;
    private final RemoteBranchInvoker invoker;
    private final JdbcTemplate jdbcTemplate;
    /** 二阶段回调里写日志用的独立事务模板（REQUIRES_NEW） */
    private final TransactionTemplate logTxTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** 事务起始时间（durationMs 统计用），回调结束时清除 */
    private final ConcurrentHashMap<String, Long> txnStart = new ConcurrentHashMap<>();

    public TccTransactionManager(TccRegistryClient registryClient,
                                 RemoteBranchInvoker invoker,
                                 JdbcTemplate jdbcTemplate,
                                 PlatformTransactionManager transactionManager) {
        this.registryClient = registryClient;
        this.invoker = invoker;
        this.jdbcTemplate = jdbcTemplate;
        this.logTxTemplate = new TransactionTemplate(transactionManager,
                new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));
    }

    /**
     * 注册一个远程分支——必须在业务方法的 @Transactional 本地事务内调用。
     * 可注册多个分支；二阶段按注册逆序对全部分支执行 Confirm/Cancel。
     *
     * @param result 业务方法正在组装的返回结果（回调执行完毕后才是完整内容）
     */
    public void registerBranch(String xid, String actionName, Map<String, Object> params,
                               Map<String, Object> result) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("registerBranch 必须在 @Transactional 本地事务内调用");
        }

        // 1. 查注册服务：动作在哪儿、由谁实现（查不到快速失败 -> 调用方标记回滚）
        TccActionInfo info = registryClient.lookup(actionName);
        log.info("[TCC] ===== 全局事务注册分支 xid={} action={} -> {} =====",
                xid, actionName, info.getServiceName());

        txnStart.putIfAbsent(xid, System.currentTimeMillis());

        // 2. 分支落库（独立事务，先于 Try）：方法快照 + 参数快照，二阶段与后台恢复都按它回放。
        //    不能随业务事务——本地回滚时它必须还在，Cancel 才找得到分支
        insertBranchLog(xid, actionName, info, params);
        result.putIfAbsent("actionName", actionName);

        // 3. 首个分支：全局 TRYING 日志（随业务事务，提交才有记录）+ 事务回调（一个事务只挂一次）
        registerCompletionCallbackOnce(xid, result);

        // 4. 远程 Try：统一信封调用参与方通用端点
        TccRemoteResult r = invoker.invoke(info, xid, "TRY", params);
        result.putIfAbsent("tryMessage", r.getMessage());

        if (!r.isSuccess()) {
            // 不抛异常（抛出会成为 Controller 的 500）：标记回滚 -> 方法正常返回
            // -> Spring 回滚 -> afterCompletion(ROLLED_BACK) -> 逆序 Cancel
            log.warn("[TCC] 远程 Try 失败({})，标记本地事务回滚（rollbackOnly）", r.getMessage());
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
        }
    }

    /** 一个事务只注册一次回调（多分支场景）；首个分支同时写全局 TRYING 日志（随业务事务） */
    private void registerCompletionCallbackOnce(String xid, Map<String, Object> result) {
        boolean already = false;
        for (TransactionSynchronization s : TransactionSynchronizationManager.getSynchronizations()) {
            if (s instanceof TxCompletionCallback) {
                already = true;
                break;
            }
        }
        if (!already) {
            // 全局事务开始标记：随本地业务事务写入（提交才有记录；回滚则由 Cancel 路径补记终态）
            saveGlobalLog(xid, "TRYING", "本地事务进行中，远程分支 Try 已发起", 0);
            TransactionSynchronizationManager.registerSynchronization(new TxCompletionCallback(xid, result));
        }
    }

    /** 事务完成回调：本地事务结局驱动远程二阶段 */
    private class TxCompletionCallback implements TransactionSynchronization {
        private final String xid;
        private final Map<String, Object> result;

        TxCompletionCallback(String xid, Map<String, Object> result) {
            this.xid = xid;
            this.result = result;
        }

        @Override
        public void afterCommit() {
            confirmAllBranches(xid, result);
        }

        @Override
        public void afterCompletion(int status) {
            if (status == TransactionSynchronization.STATUS_ROLLED_BACK) {
                cancelAllBranches(xid, result);
            }
            // STATUS_COMMITTED 时 afterCommit 已处理
        }
    }

    // ---------------- 二阶段：逆序遍历全部分支 ----------------

    /** 本地事务已提交 -> 逆序对全部分支 Confirm（各自重试，不回滚） */
    private void confirmAllBranches(String xid, Map<String, Object> result) {
        try {
            log.info("[TCC] 本地事务已提交 -> 远程 Confirm（逆序）xid={}", xid);
            List<Map<String, Object>> branches = loadBranches(xid);

            Map<String, String> states = new LinkedHashMap<>();
            int totalAttempts = 0;
            boolean allOk = true;
            String lastError = null;
            for (Map<String, Object> branch : branches) {
                String actionName = (String) branch.get("ACTION_NAME");
                TccPhaseResult rc = runPhase(actionName, xid, "CONFIRM", branch, CONFIRM_MAX_RETRY);
                totalAttempts += rc.getAttempts();
                states.put(actionName, rc.isSuccess() ? "CONFIRMED" : "CONFIRM_FAILED");
                if (!rc.isSuccess()) {
                    allOk = false;
                    lastError = rc.getMessage();
                }
            }

            // lambda 只能捕获最终变量，累加器在此取最终值
            final int tries = totalAttempts;

            if (allOk) {
                logTxTemplate.executeWithoutResult(s -> updateGlobalLog(xid, "CONFIRMED",
                        "本地事务已提交，" + branches.size() + " 个分支 Confirm 成功（共尝试 " + tries + " 次）",
                        tries));
                log.info("[TCC] ===== 全局事务提交成功 xid={} =====", xid);
                result.put("success", true);
                result.put("globalStatus", "CONFIRMED");
                result.put("orderStatus", "CREATED");
                result.put("branchStates", states);
                result.put("retryCount", tries);
                result.put("message", "下单成功：本地事务已提交（订单生效），" + branches.size()
                        + " 个远程分支全部 Confirm，库存冻结量已转为实际扣减");
            } else {
                logTxTemplate.executeWithoutResult(s -> updateGlobalLog(xid, "CONFIRM_FAILED",
                        "Confirm 重试耗尽（共尝试 " + tries + " 次），待后台恢复", tries));
                log.error("[TCC] Confirm 失败 xid={}（共尝试 {} 次），全局事务标记为 CONFIRM_FAILED（等待恢复，不回滚）",
                        xid, tries);
                result.put("success", false);
                result.put("globalStatus", "CONFIRM_FAILED");
                result.put("orderStatus", "CREATED");
                result.put("branchStates", states);
                result.put("retryCount", tries);
                result.put("message", "本地事务已提交，但远程 Confirm 未全部成功（已重试 " + tries
                        + " 次，最后原因：" + lastError + "），按 TCC 语义不回滚，等待后续恢复重试");
            }
            finishResult(xid, result);
        } catch (Exception e) {
            // 回调绝不能向外抛（事务已提交），记日志留待人工/后台处理
            log.error("[TCC] Confirm 回调异常 xid={}：{}", xid, e.getMessage(), e);
        }
    }

    /** 本地事务已回滚 -> 逆序对全部分支 Cancel（各自重试，不放弃） */
    private void cancelAllBranches(String xid, Map<String, Object> result) {
        try {
            log.warn("[TCC] 本地事务已回滚 -> 远程 Cancel（逆序）xid={}", xid);
            result.putIfAbsent("tryMessage", "Try 未发起或未成功");
            List<Map<String, Object>> branches = loadBranches(xid);

            Map<String, String> states = new LinkedHashMap<>();
            int totalAttempts = 0;
            boolean allOk = true;
            String lastError = null;
            for (Map<String, Object> branch : branches) {
                String actionName = (String) branch.get("ACTION_NAME");
                TccPhaseResult rc = runPhase(actionName, xid, "CANCEL", branch, CANCEL_MAX_RETRY);
                totalAttempts += rc.getAttempts();
                states.put(actionName, rc.isSuccess() ? "CANCELLED" : "CANCEL_FAILED");
                if (!rc.isSuccess()) {
                    allOk = false;
                    lastError = rc.getMessage();
                }
            }

            // lambda 只能捕获最终变量，累加器在此取最终值
            final int tries = totalAttempts;

            if (allOk) {
                logTxTemplate.executeWithoutResult(s -> saveGlobalLog(xid, "CANCELLED",
                        "本地事务回滚，" + branches.size() + " 个分支 Cancel 成功（共尝试 " + tries + " 次）",
                        tries));
                log.info("[TCC] 全局 Cancel 完成（共尝试 {} 次）：订单未落库，远程预留已释放", tries);
                result.put("success", false);
                result.put("globalStatus", "CANCELLED");
                result.put("orderStatus", "NONE");
                result.put("branchStates", states);
                result.put("retryCount", tries);
                result.put("message", "本地事务已回滚（远程 Cancel 尝试 " + tries
                        + " 次）：订单未落库，库存已解冻或无变化");
            } else {
                logTxTemplate.executeWithoutResult(s -> saveGlobalLog(xid, "CANCEL_FAILED",
                        "Cancel 重试耗尽（共尝试 " + tries + " 次），待后台恢复", tries));
                log.error("[TCC] Cancel 失败 xid={}（共尝试 {} 次），全局事务标记为 CANCEL_FAILED（预留资源未释放，等待恢复）",
                        xid, tries);
                result.put("success", false);
                result.put("globalStatus", "CANCEL_FAILED");
                result.put("orderStatus", "NONE");
                result.put("branchStates", states);
                result.put("retryCount", tries);
                result.put("message", "本地事务已回滚，但远程 Cancel 未全部成功（已重试 " + tries
                        + " 次，最后原因：" + lastError + "），预留资源未释放，等待后台恢复重试");
            }
            finishResult(xid, result);
        } catch (Exception e) {
            // 回调绝不能向外抛（事务已回滚），记日志留待人工/后台处理
            log.error("[TCC] Cancel 回调异常 xid={}：{}", xid, e.getMessage(), e);
        }
    }

    /** 取分支（按注册逆序）并对单个分支执行带重试的二阶段 */
    private TccPhaseResult runPhase(String actionName, String xid, String phase,
                                    Map<String, Object> branchRow, int maxRetry) {
        Map<String, Object> params = parseParams((String) branchRow.get("PARAMS"));
        TccActionInfo info = registryClient.lookupQuiet(actionName);
        if (info == null) {
            // 注册服务查不到（不可达或动作被下线）：按分支失败处理，等后台恢复
            log.error("[TCC] 分支 {} 阶段 {} 失败：注册服务中查不到该动作", actionName, phase);
            recordBranchPhase(xid, actionName, phase + "_FAILED", 0, "注册服务中查不到动作 " + actionName);
            return new TccPhaseResult(false, "UNKNOWN", "注册服务中查不到动作 " + actionName, 0);
        }

        TccRemoteResult r = null;
        int attempts = 0;
        for (int i = 1; i <= maxRetry; i++) {
            attempts = i;
            r = invoker.invoke(info, xid, phase, params);
            if (r.isSuccess()) {
                break;
            }
            log.warn("[TCC] 分支 {} 阶段 {} 第 {} 次失败({})，准备重试", actionName, phase, i, r.getMessage());
            sleep(RETRY_INTERVAL_MS);
        }
        boolean ok = r != null && r.isSuccess();
        String state = !ok ? phase + "_FAILED" : ("CONFIRM".equals(phase) ? "CONFIRMED" : "CANCELLED");
        recordBranchPhase(xid, actionName, state, attempts, ok ? null : (r == null ? "无响应" : r.getMessage()));
        return new TccPhaseResult(ok, r == null ? "UNKNOWN" : r.getState(),
                r == null ? "无响应" : r.getMessage(), attempts);
    }

    /** 分支二阶段终态与尝试次数落库（独立事务，回调里安全） */
    private void recordBranchPhase(String xid, String actionName, String state, int attempts, String error) {
        String countColumn = state.startsWith("CANCEL") ? "cancel_count" : "confirm_count";
        logTxTemplate.executeWithoutResult(s -> jdbcTemplate.update(
                "UPDATE t_global_branch_log SET branch_state = ?, " + countColumn + " = ?, last_error = ?, " +
                        "updated_at = CURRENT_TIMESTAMP WHERE xid = ? AND action_name = ?",
                state, attempts, error, xid, actionName));
    }

    private List<Map<String, Object>> loadBranches(String xid) {
        // id 倒序 = 注册逆序：Try 先 A 后 B，回滚时先 B 后 A
        return jdbcTemplate.queryForList(
                "SELECT id, action_name, method_info, params FROM t_global_branch_log " +
                        "WHERE xid = ? ORDER BY id DESC", xid);
    }

    private void finishResult(String xid, Map<String, Object> result) {
        Long start = txnStart.remove(xid);
        result.put("durationMs", System.currentTimeMillis() - (start == null ? System.currentTimeMillis() : start));
    }

    // ---------------- 分支 / 全局日志 ----------------

    /** 分支落库（独立事务）：在 Try 之前写入，回滚后仍可查到，Cancel 才有依据 */
    private void insertBranchLog(String xid, String actionName, TccActionInfo info, Map<String, Object> params) {
        logTxTemplate.executeWithoutResult(s -> jdbcTemplate.update(
                "INSERT INTO t_global_branch_log(xid, action_name, branch_state, method_info, params) " +
                        "VALUES (?, ?, 'TRIED', ?, ?)",
                xid, actionName, info.methodInfo(), toJson(params)));
    }

    private String toJson(Map<String, Object> params) {
        try {
            return objectMapper.writeValueAsString(params == null ? new LinkedHashMap<String, Object>() : params);
        } catch (Exception e) {
            return "{}";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseParams(String json) {
        try {
            if (json == null || json.isEmpty()) {
                return new LinkedHashMap<>();
            }
            return objectMapper.readValue(json, Map.class);
        } catch (Exception e) {
            log.warn("[TCC] 分支参数快照解析失败：{}", e.getMessage());
            return new LinkedHashMap<>();
        }
    }

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
