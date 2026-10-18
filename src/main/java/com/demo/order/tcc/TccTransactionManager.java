package com.demo.order.tcc;

import com.demo.order.client.StorageClient;
import com.demo.order.client.TccRemoteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * TCC 远程分支协调器：只负责对远程服务（storage-service）的调用与失败重试。
 *
 * 职责边界：
 *   - 本地订单分支的事务由 OrderService 委托 OrderTccService 处理，
 *     其原子性由 Spring 事务管理器控制，不需要也不应经过本类；
 *   - 远程调用存在网络超时、服务抖动等不确定性，二阶段（Confirm/Cancel）
 *     必须重试到成功为止，重试次数上限、间隔与终态判定都在这里收敛。
 *
 * 二阶段重试语义（与 Seata TC 对远程分支的处理一致）：
 *   - Confirm 失败：重试，不回滚（Try 都已成功，回滚反而制造不一致）
 *   - Cancel  失败：重试，不放弃（Try 已成功分支的预留资源必须被释放）
 *   - 重试耗尽：返回失败结果，由调用方（OrderService）记录 CANCEL_FAILED / CONFIRM_FAILED 终态
 */
@Component
public class TccTransactionManager {

    private static final Logger log = LoggerFactory.getLogger(TccTransactionManager.class);

    /** Confirm 阶段失败重试次数（真实框架会由后台任务持续重试） */
    private static final int CONFIRM_MAX_RETRY = 3;
    /** Cancel 阶段失败重试次数（与 Confirm 同等对待，回滚也必须最终成功） */
    private static final int CANCEL_MAX_RETRY = 3;
    private static final long RETRY_INTERVAL_MS = 500L;

    private final StorageClient storageClient;

    public TccTransactionManager(StorageClient storageClient) {
        this.storageClient = storageClient;
    }

    /** 远程库存分支 Try：冻结库存（单次调用，失败即触发全局回滚） */
    public TccRemoteResult tryDeduct(String xid, long productId, int count, boolean mockFail) {
        log.info("[TCC] Try 阶段 -> 库存分支(远程)：冻结商品 {} 库存 {} 件", productId, count);
        return storageClient.tryDeduct(xid, productId, count, mockFail);
    }

    /** 远程库存分支 Confirm：冻结转实扣（失败重试，不回滚） */
    public TccPhaseResult confirm(String xid, boolean mockFailConfirm) {
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

    /** 远程库存分支 Cancel：解冻归还（失败重试，不放弃） */
    public TccPhaseResult cancel(String xid, boolean mockFailCancel) {
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

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
