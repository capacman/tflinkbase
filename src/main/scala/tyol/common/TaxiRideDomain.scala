package tyol.common

import java.time.Instant

object TaxiRideDomain {
  case class TaxiRide(
      rideId: Long,
      isStart: Boolean,
      eventTime: Instant,
      startLon: Float,
      startLat: Float,
      endLon: Float,
      endLat: Float,
      passengerCnt: Short,
      taxiId: Long,
      driverId: Long
  )
  object TaxiRide {
    def apply(rideId: Long, isStart: Boolean): TaxiRide = {
      val g = new Generator(rideId)
      TaxiRide(
        rideId,
        isStart,
        if (isStart) g.startTime() else g.endTime(),
        g.startLon,
        g.startLat,
        g.endLon,
        g.endLat,
        g.passengerCnt,
        g.taxiId,
        g.driverId
      )
    }
  }
}
