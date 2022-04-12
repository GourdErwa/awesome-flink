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
    private long second = System.currentTimeMillis();
    private String userId;
    private String ip;
    private String type;

    public Event(String userId, String ip, String type) {
        this.userId = userId;
        this.ip = ip;
        this.type = type;
    }
}
