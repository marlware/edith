package app.marlware.edith

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{MapFunction, OpenContext}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import redis.clients.jedis.JedisPooled

/**
 * Reads ad impressions Agnes publishes to Kafka and keeps a running count per ad.
 * Counts land in the Redis hash `impressions:byAd` (field = ad id) for Edith to serve.
 *
 * Reads from the earliest offset with no checkpoints, so a restart recomputes the same
 * totals rather than double counting.
 */
object ImpressionJob {

  def main(args: Array[String]): Unit = {
    val kafka = sys.env.getOrElse("KAFKA_URL", "kafka:19092")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "redis")

    val source = KafkaSource
      .builder[String]()
      .setBootstrapServers(kafka)
      .setTopics("ad-impressions")
      .setGroupId("edith-flink")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .fromSource(source, WatermarkStrategy.noWatermarks[String](), "ad-impressions")
      .map(new ExtractAdId)
      .returns(Types.STRING)
      .keyBy(new Identity, Types.STRING)
      .process(new RunningCount)
      .returns(Types.TUPLE[Tuple2[String, java.lang.Long]](Types.STRING, Types.LONG))
      .sinkTo(new RedisSink(redisHost))

    env.execute("edith-impressions")
  }
}

class ExtractAdId extends MapFunction[String, String] {
  @transient private lazy val mapper = new ObjectMapper()
  override def map(message: String): String = mapper.readTree(message).get("adId").asText()
}

class Identity extends KeySelector[String, String] {
  override def getKey(adId: String): String = adId
}

class RunningCount extends KeyedProcessFunction[String, String, Tuple2[String, java.lang.Long]] {
  private var count: ValueState[java.lang.Long] = _

  override def open(ctx: OpenContext): Unit =
    count = getRuntimeContext.getState(new ValueStateDescriptor("count", Types.LONG))

  override def processElement(
      adId: String,
      ctx: KeyedProcessFunction[String, String, Tuple2[String, java.lang.Long]]#Context,
      out: Collector[Tuple2[String, java.lang.Long]]
  ): Unit = {
    val next: java.lang.Long = Option(count.value()).map(_.longValue()).getOrElse(0L) + 1L
    count.update(next)
    out.collect(Tuple2.of(adId, next))
  }
}

class RedisSink(host: String) extends Sink[Tuple2[String, java.lang.Long]] {
  override def createWriter(ctx: WriterInitContext): SinkWriter[Tuple2[String, java.lang.Long]] =
    new SinkWriter[Tuple2[String, java.lang.Long]] {
      private val redis = new JedisPooled(host, 6379)

      override def write(value: Tuple2[String, java.lang.Long], ctx: SinkWriter.Context): Unit =
        redis.hset("impressions:byAd", value.f0, value.f1.toString)

      override def flush(endOfInput: Boolean): Unit = ()

      override def close(): Unit = redis.close()
    }
}
