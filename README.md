# flink-advanced
flink 实用示例代码。

# 环境
Flink 1.14.4

# flink-stream 模块
> 示例代码因 [flink-cdc](https://github.com/GourdErwa/flink-cdc-connectors) 版本问题与 flink 版本不兼容，需自行编译 cdc 适配 flin 版本。

- [flink-cdc](flink-stream/src/main/java/io/group/flink/stream/cdc) 自定义序列化器
- flink-cdc changelog 按规则[分流写入](flink-stream/src/main/java/io/group/flink/stream/sink/ChangelogSink.java)不同表


