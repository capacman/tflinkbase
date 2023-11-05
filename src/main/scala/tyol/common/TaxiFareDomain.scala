package tyol.common

import java.time.Instant

object TaxiFareDomain {
  case class TaxiFare(
      rideId: Long,
      taxiId: Long,
      driverId: Long,
      startTime: Instant,
      paymentType: String,
      tip: Float,
      tolls: Float,
      totalFare: Float
  )
  object TaxiFare {
    def apply(rideId: Long): TaxiFare = {
      val g = new Generator(rideId)
      TaxiFare(
        rideId,
        g.taxiId,
        g.driverId,
        g.startTime(),
        g.paymentType,
        g.tip,
        g.tolls,
        g.totalFare
      )
    }
  }
}
