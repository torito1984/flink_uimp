package com.flink.iot.basics.entities

object CustomerEntities {
  case class CustomerReport(timestamp: Long,
                            vehicleId: Long,
                            customerName: String,
                            value: String)
}
