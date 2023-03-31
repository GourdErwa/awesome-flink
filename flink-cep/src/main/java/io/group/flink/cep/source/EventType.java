package io.group.flink.cep.source;

/**
 * 事件类型
 *
 * @author Li.Wei by 2022/4/12
 */
public enum EventType {
    /**
     * Login event type.
     */
    LOGIN,
    /**
     * Logout event type.
     */
    LOGOUT,
    /**
     * Click event type.
     */
    CLICK,
    /**
     * Pay event type.
     */
    PAY
}
