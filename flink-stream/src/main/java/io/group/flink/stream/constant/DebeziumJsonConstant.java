package io.group.flink.stream.constant;

import io.debezium.data.Envelope;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Debezium 序列化相关字段常量
 *
 * @author Li.Wei by 2022/6/13
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DebeziumJsonConstant {

    public static final String SOURCE = Envelope.FieldName.SOURCE;
    public static final String TABLE = "table";
    public static final String DB = "db";

    public static final String AFTER = Envelope.FieldName.AFTER;
    public static final String BEFORE = Envelope.FieldName.BEFORE;

}
