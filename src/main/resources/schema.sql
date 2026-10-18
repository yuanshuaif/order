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
