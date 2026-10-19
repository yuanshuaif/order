package com.demo.order.tcc.remote;

/**
 * 从注册服务查到的 TCC 动作信息（发起方视角）
 *
 * 发起方只认 actionName；方法在哪儿、由谁实现，全从这里查。
 * JSON 字段与 tcc-registry-service 的 TccActionRegistration 一致。
 */
public class TccActionInfo {

    private String actionName;
    private String serviceName;
    private String serviceUrl;
    private String tryMethod;
    private String confirmMethod;
    private String cancelMethod;

    /** 审计快照：写入分支日志 method_info 列，注册信息变更后仍可追溯当时调的是谁 */
    public String methodInfo() {
        return serviceName + "#" + tryMethod + "/" + confirmMethod + "/" + cancelMethod
                + " @" + serviceUrl;
    }

    public String getActionName() {
        return actionName;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getTryMethod() {
        return tryMethod;
    }

    public void setTryMethod(String tryMethod) {
        this.tryMethod = tryMethod;
    }

    public String getConfirmMethod() {
        return confirmMethod;
    }

    public void setConfirmMethod(String confirmMethod) {
        this.confirmMethod = confirmMethod;
    }

    public String getCancelMethod() {
        return cancelMethod;
    }

    public void setCancelMethod(String cancelMethod) {
        this.cancelMethod = cancelMethod;
    }
}
