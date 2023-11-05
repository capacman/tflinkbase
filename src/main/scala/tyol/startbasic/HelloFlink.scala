package tyol.startbasic

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions._
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import tyol.common.SensorDomain
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.time.{Time => WindowTime}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.table.api.Tumble
import org.apache.flink.util.Collector

import java.lang

object HelloFlink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment
      .createLocalEnvironmentWithWebUI() // TODO remove in production
      .setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.setParallelism(2)

    // val stream: DataStream[String] = env.fromElements("Hello", "Flink", "World")

    val wms: WatermarkStrategy[SensorDomain.SensorReading] = WatermarkStrategy
      .forBoundedOutOfOrderness[SensorDomain.SensorReading](
        java.time.Duration.ofSeconds(5)
      )
      .withTimestampAssigner(new TimestampAssignerSupplier[SensorDomain.SensorReading] {

        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorDomain.SensorReading] = {
          (element: SensorDomain.SensorReading, _: Long) => element.timestamp
        }
      })

    val sensorData: DataStream[SensorDomain.SensorReading] = env
      .addSource(new SensorDomain.SensorSource)
      .assignTimestampsAndWatermarks(wms)

    /*sensorData.filter(_.temperature>30).addSink(new SinkFunction[SensorDomain.SensorReading] {
      override def invoke(value: SensorDomain.SensorReading, context: SinkFunction.Context): Unit = println(s"simplesinkbigger30: $value")
    }).name(">30")

    sensorData.filter(_.temperature < 30).addSink(new SinkFunction[SensorDomain.SensorReading] {
      override def invoke(value: SensorDomain.SensorReading, context: SinkFunction.Context): Unit = println(s"simplesinkless30: $value")
    }).name("<30")*/

    // sensorData.print()

    /*

    source id=1 --> myproc1
                \ /
                 /\id=2
                /  \
    source id=2 --------> myproc2

     */
    /*
    val keyed: KeyedStream[SensorDomain.SensorReading, String] = sensorData
      .keyBy(_.id)

    keyed
      .map(new RichMapFunction[SensorDomain.SensorReading, (String, Double)] {
        private var sum: ValueState[(String, Long, Long)] = _

        override def map(value: SensorDomain.SensorReading): (String, Double) = {
          val sumState = sum.value()

          val current =
            if (sumState == null) (value.id, 1L, value.temperature.toLong)
            else (value.id, sumState._2 + 1L, sumState._3 + value.temperature.toLong)
          sum.update(current)
          (current._1, current._3 / current._2.toDouble)
        }

        override def open(parameters: Configuration): Unit = {
          val descriptor = new ValueStateDescriptor[(String, Long, Long)](
            "sum",
            classOf[(String, Long, Long)]
          )
          descriptor.enableTimeToLive(
            StateTtlConfig
              .newBuilder(Time.days(1))
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .build()
          )
          sum = getRuntimeContext.getState(descriptor)
        }
      })
      .print()
     */

    val windowProcessFunction = new ProcessWindowFunction[SensorDomain.SensorReading, (String, Double, Long), String, TimeWindow] {

      override def process(
          key: String,
          context: Context,
          elements: Iterable[SensorDomain.SensorReading],
          out: Collector[(String, Double, Long)]
      ): Unit = {
        elements
          .reduceOption((l, r) => if (l.temperature > r.temperature) l else r)
          .foreach { maxSensorReading =>
            val outToCollect = (key, maxSensorReading.temperature, context.window.getEnd)
            out.collect(outToCollect)
          }
      }
    }
    sensorData
      .keyBy(_.id)
      .window(TumblingEventTimeWindows.of(WindowTime.seconds(60)))
      .process(windowProcessFunction)
      .windowAll(TumblingEventTimeWindows.of(WindowTime.seconds(60)))
      .maxBy(1)
      .print()

     //println(env.getExecutionPlan)
    env.execute("Hello Flink")

  }
}
