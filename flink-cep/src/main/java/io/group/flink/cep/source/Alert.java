package io.group.flink.cep.source;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Li.Wei by 2022/4/12
 */
@Data
@NoArgsConstructor
public class Alert {
    private Object object;

    public Alert(Object object) {
        this.object = object;
    }
}
