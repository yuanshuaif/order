package com.demo.order.controller;

import com.demo.order.service.OrderService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * 对外唯一的业务接口：下单（内部走 TCC 分布式事务）
 *
 * 参数：
 *   productId       商品 id（默认 1001，初始库存 100）
 *   count           购买数量（默认 1）
 *   mockFailTry     true = 模拟 Try 阶段库存分支业务失败，验证全局回滚（Cancel）
 *   mockFailConfirm true = 模拟 Confirm 阶段库存分支失败，验证 Confirm 失败重试语义
 *   mockFailCancel  true = 模拟 Cancel 阶段库存分支失败，验证 Cancel 失败重试语义
 */
@RestController
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/order/create")
    public Map<String, Object> createOrder(@RequestParam(defaultValue = "1001") long productId,
                                           @RequestParam(defaultValue = "1") int count,
                                           @RequestParam(defaultValue = "false") boolean mockFailTry,
                                           @RequestParam(defaultValue = "false") boolean mockFailConfirm,
                                           @RequestParam(defaultValue = "false") boolean mockFailCancel) {
        return orderService.placeOrder(productId, count, mockFailTry, mockFailConfirm, mockFailCancel);
    }
}
