package com.flink.iot.basics.entities

object VehicleEntities {

  case class VehicleState(vehicleId: Long,
                          moving: Int,
                          timestamp: Long)

  case class OwnerReferenceData(ownerName: String,
                                reportedStolen: Boolean)

  case class EnrichedVehicleState(state: VehicleState,
                                  owner: OwnerReferenceData)
}
