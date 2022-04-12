package io.group.flink.cep.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Li.Wei by 2022/4/12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {
    private long second;
    private String userId;
    private String ip;
    private EventType event;

    public Event(String userId, String ip, EventType event) {
        this.userId = userId;
        this.ip = ip;
        this.event = event;
    }

    public Event time(long second) {
        this.second = second;
        return this;
    }
}
