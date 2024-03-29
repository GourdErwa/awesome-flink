package io.group.flink.cep;

import io.group.flink.cep.source.Event;
import io.group.flink.cep.source.EventSource;
import io.group.flink.cep.source.EventType;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * CEP 默认使用 {@link PatternStream#inEventTime()} 时间，需要我们指定水位线。
 *
 * @author Li.Wei by 2022/4/11
 */
@Slf4j
public class EventCep {

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)) {
            final KeyedStream<Event, String> source = env.addSource(new EventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withIdleness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, timestamp) -> event.getSecond())
                ).name("事件流")
                .keyBy((KeySelector<Event, String>) Event::getUserId);

            registerPattern1(source);
            //registerPattern2(source);
            //registerPattern3(source);

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    Thread.sleep(10000L);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("load registerPattern3...");
//                registerPattern3(source);
//            }
//        }).start();
            env.execute(EventCep.class.getSimpleName());
        }
    }

    private static void registerPattern1(DataStream<Event> dataStream) {
        final String patternName = "p1";
        Pattern<Event, ?> pattern = Pattern.<Event>begin(patternName + "_LOGIN")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) throws Exception {
                    return EventType.LOGIN.equals(event.getEvent());
                }
            })
            .next(patternName + "_LOGOUT")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) throws Exception {
                    return EventType.LOGOUT.equals(event.getEvent());
                }
            })
            .within(Time.seconds(5));

        final OutputTag<Event> sideOutputTag = new OutputTag<Event>(patternName + "_late") {
        };

        final SingleOutputStreamOperator<String> streamOperator = CEP
            .pattern(dataStream, pattern)
            .inProcessingTime()
            .sideOutputLateData(sideOutputTag) // 使用处理时间不会有迟到事件
            .select((PatternSelectFunction<Event, String>) Object::toString);
        //streamOperator.print().name(patternName + "_sink");

        streamOperator
            .getSideOutput(sideOutputTag)
            .map(x -> patternName + "_out: " + x)
            .print();
    }


    private static void registerPattern2(DataStream<Event> dataStream) {
        final String patternName = "p2";
        Pattern<Event, ?> pattern2 = Pattern.<Event>begin(patternName + "_LOGIN")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) throws Exception {
                    return EventType.LOGIN.equals(event.getEvent());
                }
            })
            .next(patternName + "_CLICK")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) throws Exception {
                    return EventType.CLICK.equals(event.getEvent());
                }
            })
            .within(Time.seconds(10));
        final SingleOutputStreamOperator<String> streamOperator = CEP
            .pattern(dataStream, pattern2)
            .inProcessingTime()
            .select((PatternSelectFunction<Event, String>) Object::toString);
        streamOperator.print().name(patternName + "_sink");
        streamOperator
            .getSideOutput(new OutputTag<String>(patternName + "_late") {
            })
            .map(x -> patternName + "_out: " + x)
            .print();
    }

    private static void registerPattern3(DataStream<Event> dataStream) {
        final String patternName = "p3";
        Pattern<Event, ?> pattern2 = Pattern.<Event>begin(patternName + "_PAY")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) throws Exception {
                    return EventType.PAY.equals(event.getEvent());
                }
            })
            .within(Time.seconds(10));
        final SingleOutputStreamOperator<String> streamOperator = CEP
            .pattern(dataStream, pattern2)
            .inProcessingTime()
            .select((PatternSelectFunction<Event, String>) Object::toString);
        streamOperator.print().name(patternName + "_sink");
        streamOperator
            .getSideOutput(new OutputTag<String>(patternName + "_late") {
            })
            .map(x -> patternName + "_out: " + x)
            .print();
    }
}
