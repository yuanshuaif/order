package com.demo.order.tcc;

/**
 * 远程分支二阶段（Confirm/Cancel）的执行结果
 *
 * success ：该阶段是否最终成功（已含重试）
 * state   ：远程分支当前状态
 * message ：最后一次调用的返回信息
 * attempts：实际调用次数（含第一次）
 */
public class TccPhaseResult {

    private final boolean success;
    private final String state;
    private final String message;
    private final int attempts;

    public TccPhaseResult(boolean success, String state, String message, int attempts) {
        this.success = success;
        this.state = state;
        this.message = message;
        this.attempts = attempts;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getState() {
        return state;
    }

    public String getMessage() {
        return message;
    }

    public int getAttempts() {
        return attempts;
    }
}
