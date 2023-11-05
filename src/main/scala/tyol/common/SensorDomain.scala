package tyol.common

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.util.Calendar
import scala.util.Random

object SensorDomain {
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  class SensorSource(val location:String="Default",lateArrivalPercent:Int=0,lateValue:Int=20000) extends RichParallelSourceFunction[SensorReading] {

    // flag indicating whether source is still running.
    var running: Boolean = true

    /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
    override def run(srcCtx: SourceContext[SensorReading]): Unit = {

      // initialize random number generator
      val rand = new Random()
      // look up index of this parallel task
      val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

      // initialize sensor ids and temperatures
      val maxSensorPerTask = 2
      var curFTemp = (1 to maxSensorPerTask).map { i =>
        (s"sensor_${location}_" + (taskIdx * maxSensorPerTask + i), 65 + (rand.nextGaussian() * 20))
      }

      /*Stream.continually(0).takeWhile(_ => running).foldLeft(curFTemp){ (z,_) =>
        val curTime = Calendar.getInstance.getTimeInMillis
        curFTemp.foreach(t =>
          srcCtx.collect(SensorReading(t._1, curTime, t._2))
        )
        Thread.sleep(100)
        curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      }*/
      // emit data until being canceled
      while (running) {

        // update temperature
        curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
        // get current time
        val curTime = Calendar.getInstance.getTimeInMillis

        // emit new SensorReading
        curFTemp.foreach(t =>
          if (rand.nextInt(100) < lateArrivalPercent) {
            srcCtx.collect(SensorReading(t._1, curTime - rand.nextInt(lateValue), t._2))
          } else {
            srcCtx.collect(SensorReading(t._1, curTime, t._2))
          }
        )

        // wait for 100 ms
        Thread.sleep(100)
      }

    }

    /** Cancels this SourceFunction. */
    override def cancel(): Unit = {
      running = false
    }

  }
}
