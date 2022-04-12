package io.group.flink.cep.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author Li.Wei by 2022/4/12
 */
public class EventSource implements SourceFunction<Event> {
    private static final List<Event> DATA = Arrays.asList(
        new Event("小明", "192.168.0.1", "fail"),
        new Event("小明", "192.168.0.2", "fail"),
        new Event("小王", "192.168.10,11", "fail"),
        new Event("小王", "192.168.10,12", "fail"),
        new Event("小明", "192.168.0.3", "fail"),
        new Event("小明", "192.168.0.4", "fail"),
        new Event("小王", "192.168.10,10", "success")
    );

    private boolean flag = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        while (flag) {
            DATA.forEach(ctx::collect);
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
