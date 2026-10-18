package com.demo.order.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 订单分支（TCC 本地参与方，RM 角色）
 *
 * 三个方法的资源语义：
 *   Try    —— 预留资源：插入订单，状态置 TRYING（表示“占住了”这个下单资格）
 *   Confirm —— 提交事务：订单状态 TRYING -> CONFIRMED（订单正式生效）
 *   Cancel  —— 回滚事务：订单状态 TRYING -> CANCELLED（释放预留资格）
 *
 * 每个方法是一个独立的本地事务，由 Spring 事务管理器（@Transactional）控制：
 * Try/Confirm/Cancel 各自独立提交，这正是 TCC 一阶段即提交本地事务的语义。
 *
 * 所有方法都天然幂等：UPDATE 带 status='TRYING' 条件，已处于终态时影响 0 行，重复调用安全。
 */
@Service
public class OrderTccService {

    private final JdbcTemplate jdbcTemplate;

    public OrderTccService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** Try：创建订单（状态 TRYING） */
    @Transactional
    public void tryCreateOrder(String xid, long productId, int count) {
        Integer existing = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM t_order WHERE xid = ?", Integer.class, xid);
        if (existing != null && existing > 0) {
            // 幂等：同一 xid 重复 Try 直接返回，不重复下单
            return;
        }
        jdbcTemplate.update(
                "INSERT INTO t_order(xid, product_id, order_count, status) VALUES (?, ?, ?, 'TRYING')",
                xid, productId, count);
    }

    /** Confirm：订单生效（TRYING -> CONFIRMED） */
    @Transactional
    public void confirmOrder(String xid) {
        jdbcTemplate.update(
                "UPDATE t_order SET status = 'CONFIRMED' WHERE xid = ? AND status = 'TRYING'", xid);
    }

    /** Cancel：取消订单（TRYING -> CANCELLED），释放预留资源 */
    @Transactional
    public void cancelOrder(String xid) {
        jdbcTemplate.update(
                "UPDATE t_order SET status = 'CANCELLED' WHERE xid = ? AND status = 'TRYING'", xid);
    }
}
