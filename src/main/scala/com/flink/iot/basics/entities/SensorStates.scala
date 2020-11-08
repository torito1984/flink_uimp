package com.flink.iot.basics.entities

object SensorStates {
  sealed trait SensorState
  case class SensorOpen() extends SensorState
  case class SensorClose() extends SensorState
  case class SensorObstructed() extends SensorState
}

