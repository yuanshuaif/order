-- 订单表：status 取值 TRYING / CONFIRMED / CANCELLED
CREATE TABLE IF NOT EXISTS t_order (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    xid         VARCHAR(64) NOT NULL,
    product_id  BIGINT      NOT NULL,
    order_count INT         NOT NULL,
    status      VARCHAR(16) NOT NULL,
    created_at  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- 全局事务日志表（协调器记录）：status 取值 TRYING / CONFIRMED / CANCELLED / CONFIRM_FAILED
CREATE TABLE IF NOT EXISTS t_global_txn_log (
    xid        VARCHAR(64) PRIMARY KEY,
    status     VARCHAR(16) NOT NULL,
    detail     VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
