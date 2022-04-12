package io.group.flink.cep;

import io.group.flink.cep.source.Event;
import io.group.flink.cep.source.EventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Li.Wei by 2022/4/11
 */
@Slf4j
public class EventCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .createLocalEnvironmentWithWebUI(new Configuration());
        final DataStreamSource<Event> streamSource = env.addSource(new EventSource());
        final WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
            .withIdleness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, timestamp) -> event.getSecond());
        streamSource.assignTimestampsAndWatermarks(watermarkStrategy);

        // 在3秒 内重复登录了三次, 则产生告警
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) throws Exception {
                    System.out.println("first: " + event);
                    return true;
                }
            });
        //.within(Time.seconds(10));
        // 根据用户id分组，以便可以锁定用户IP，cep模式匹配
        PatternStream<Event> patternStream = CEP.pattern(streamSource, pattern);

        patternStream
            .process(new PatternProcessFunction<Event, Event>() {
                @Override
                public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Event> out) throws Exception {
                    for (List<Event> value : match.values()) {
                        for (Event event : value) {
                            System.out.println(event);
                            out.collect(event);
                        }
                    }
                }
            })
            .print()
            .setParallelism(1);

        env.execute(EventCep.class.getSimpleName());
    }

}
