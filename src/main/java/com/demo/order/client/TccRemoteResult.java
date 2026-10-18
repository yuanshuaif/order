package com.demo.order.client;

/**
 * 库存服务 TCC 接口的统一响应
 *
 * success：本阶段是否成功
 * state  ：库存分支当前状态 TRIED / CONFIRMED / CANCELLED / EMPTY_CANCELLED
 * message：说明信息
 */
public class TccRemoteResult {

    private boolean success;
    private String state;
    private String message;

    public TccRemoteResult() {
    }

    public TccRemoteResult(boolean success, String state, String message) {
        this.success = success;
        this.state = state;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
