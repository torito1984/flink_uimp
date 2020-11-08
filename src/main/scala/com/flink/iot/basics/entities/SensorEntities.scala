package com.flink.iot.basics.entities

object SensorEntities {

  trait Sensor {
    val timestamp: Long
    val sensorId: Long
    val units: String
    val value: Double
  }

  case class SensorMeasurement(timestamp: Long,
                               sensorId: Long,
                               units: String,
                               value: Double) extends Sensor

  case class SensorAlert(timestamp: Long,
                         sensorId: Long,
                         units: String,
                         value: Double,
                         level: Int) extends Sensor

  case class SensorValue(`type`: String,
                         timestamp: Long,
                         sensorId: Long,
                         units: String,
                         value: Double) extends Sensor

  case class SecondarySensorMeasurement(timestamp: Long,
                                        sensorId: Long,
                                        units: String,
                                        value: Double) extends Sensor

  case class MAValue(sensorId: Long = 0,
                     timestamp: Long = 0,
                     count: Double = 0,
                     cum: Double= 0 )

  case class SensorReferenceData(sensorId: Long,
                                 lon: Double,
                                 lat: Double,
                                 importantMetadata: String,
                                 moreImportantMetadata: String)

  case class EnrichedMeasurements(sensorMeasurement: SensorMeasurement,
                                  sensorReferenceData: SensorReferenceData)
}
