package tyol.startbasic

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions._
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import tyol.common.SensorDomain
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.util.Collector

object HelloFlink {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment
      .createLocalEnvironmentWithWebUI() // TODO remove in production
      .setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.setParallelism(1)

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

    val sensorData = env
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

    sensorData
      .keyBy(_.id)
      .map(new RichMapFunction[SensorDomain.SensorReading, (String, Long)] {
        private var sum: ValueState[(String, Long)] = _

        override def map(value: SensorDomain.SensorReading): (String, Long) = {
          val (id, count) = sum.value()

          val current = (value.id, count + 1)
          sum.update(current)
          current
        }

        override def open(parameters: Configuration): Unit = {
          val descriprtor = new ValueStateDescriptor[(String, Long)](
            "sum",
            classOf[(String, Long)],
            ("", 0L)
          )
          sum = getRuntimeContext.getState(descriprtor)
        }
      }).print()

    env.execute("Hello Flink")
  }
}
