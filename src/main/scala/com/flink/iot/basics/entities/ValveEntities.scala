package com.flink.iot.basics.entities

import com.flink.iot.basics.entities.SensorStates._

object ValveEntities {

  case class ValveState(timestamp: Long,
                        sensorId: Long,
                        value: SensorState)

  case class MeanValveBehavior(pctOpen: Double,
                               pctClosed: Double,
                               pctObstructed: Double,
                               meanTemperature: Double)
}
