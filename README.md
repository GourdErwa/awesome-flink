# Flink-advanced
Flink 实用示例代码。

# 环境
- JDK 11
- Flink 1.16.1

# Flink-stream 模块
> 示例代码因 [Flink-cdc](https://github.com/GourdErwa/flink-cdc-connectors) 版本问题与 Flink 版本不兼容，需自行编译 cdc 适配 Flink 版本。

- [Flink-cdc](flink-stream/src/main/java/io/group/flink/stream/cdc) 自定义序列化器
- Flink-cdc changelog 按规则[分流写入](flink-stream/src/main/java/io/group/flink/stream/sink/ChangelogSink.java)不同表


