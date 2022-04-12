package io.group.flink.cep.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

import static io.group.flink.cep.source.EventType.*;

/**
 * @author Li.Wei by 2022/4/12
 */
public class EventSource implements SourceFunction<Event> {

    private static final List<Event> DATA = Arrays.asList(
        new Event("小明", "192.168.0.1", LOGIN),
        new Event("小明", "192.168.0.1", LOGOUT),
        new Event("小明", "192.168.0.2", LOGIN),
        new Event("小明", "192.168.0.2", CLICK),
        new Event("小明", "192.168.0.2", PAY),
        new Event("小明", "192.168.0.2", LOGOUT),
        new Event("小明", "192.168.0.3", LOGIN)
    );

    private boolean flag = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (flag) {
            for (Event e : DATA) {
                ctx.collect(e.time(System.currentTimeMillis() / 1000));
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
