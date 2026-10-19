-- 订单表：无状态列——订单随本地事务提交而存在、回滚而消失，没有 TRYING/CANCELLED 中间态
CREATE TABLE IF NOT EXISTS t_order (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    xid         VARCHAR(64) NOT NULL,
    product_id  BIGINT      NOT NULL,
    order_count INT         NOT NULL,
    created_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- 全局事务日志表（协调器记录）
-- status 取值 TRYING / CONFIRMED / CANCELLED / CONFIRM_FAILED / CANCEL_FAILED
-- TRYING 与本地业务在同一事务写入（提交才有记录）；回滚路径的终态在事务外补记
-- retry_count 记录二阶段（Confirm 或 Cancel）实际尝试次数，供排查与后台恢复使用
CREATE TABLE IF NOT EXISTS t_global_txn_log (
    xid         VARCHAR(64) PRIMARY KEY,
    status      VARCHAR(16) NOT NULL,
    detail      VARCHAR(255),
    retry_count INT         NOT NULL DEFAULT 0,
    created_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- 全局分支日志表（协调器记录的"事务参与者清单"，对应 Seata TC 的 branch_table）
-- 注册远程分支时以【独立事务】写入且先于 Try 发起：分支日志记录的是"远程副作用已发生"的事实，
-- 必须活过本地事务回滚（否则回滚后查不到分支，Cancel 发不出去，已冻结资源无人释放）；
-- 先于 Try 写则 Try 中途崩溃也有 Cancel 兜底（参与方以空回滚应对）。
-- 二阶段回调与后台恢复都按它逆序回放：调谁、调哪个方法、用什么参数。
-- method_info 是注册信息的审计快照（服务名#Try/Confirm/Cancel方法 @地址），
-- 注册表后续变更不影响已落库分支的可追溯性。
-- branch_state 取值 TRIED / CONFIRMED / CONFIRM_FAILED / CANCELLED / CANCEL_FAILED
CREATE TABLE IF NOT EXISTS t_global_branch_log (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    xid          VARCHAR(64)  NOT NULL,
    action_name  VARCHAR(64)  NOT NULL,
    branch_state VARCHAR(16)  NOT NULL,
    method_info  VARCHAR(255),
    params       VARCHAR(1000),
    confirm_count INT         NOT NULL DEFAULT 0,
    cancel_count  INT         NOT NULL DEFAULT 0,
    last_error   VARCHAR(255),
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_branch UNIQUE (xid, action_name)
);
