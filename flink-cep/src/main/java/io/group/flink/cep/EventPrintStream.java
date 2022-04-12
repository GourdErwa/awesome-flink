package io.group.flink.cep;

import io.group.flink.cep.source.Event;
import io.group.flink.cep.source.EventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author Li.Wei by 2022/4/11
 */
@Slf4j
public class EventPrintStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .createLocalEnvironmentWithWebUI(new Configuration());
        final DataStreamSource<Event> streamSource = env.addSource(new EventSource());
        final WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withIdleness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, timestamp) -> event.getSecond());
        streamSource.assignTimestampsAndWatermarks(watermarkStrategy);

        streamSource.map(new MapFunction<Event, Event>() {
            @Override
            public Event map(Event value) throws Exception {
                return value;
            }
        });
        streamSource.print();

        env.execute(EventPrintStream.class.getSimpleName());
    }

}
