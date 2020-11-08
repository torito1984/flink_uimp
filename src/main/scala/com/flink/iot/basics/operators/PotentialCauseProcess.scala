package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.CustomerEntities._
import com.flink.iot.basics.entities.SensorEntities._
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

/**
 * Para intentar determinar una causa de error, mantenemos la ultima queja de propietario y la ultima alerta del vehiculo.
 * En el caso de detectar un caso en el que notificacion y alerta esta poximas en el tiempo notificamos una potencial
 * causa de la queja.
 */
class PotentialCauseProcess extends KeyedCoProcessFunction[Long, CustomerReport, SensorAlert, (CustomerReport, SensorAlert)] {

  private var latestComplaint: ValueState[CustomerReport] = _
  private var latestAlert: ValueState[SensorAlert] = _

  override def open(parameters: Configuration): Unit = {
    val complaintDescriptor =
      new ValueStateDescriptor(
        "latestComplaint", // the state name
        classOf[CustomerReport]) // state class
    latestComplaint = getRuntimeContext.getState(complaintDescriptor)

    val alertDescriptor =
      new ValueStateDescriptor(
        "latestAlert", // the state name
        classOf[SensorAlert]) // state class
    latestAlert = getRuntimeContext.getState(alertDescriptor)
  }

  override def processElement1(customerReport: CustomerReport,
                               ctx: KeyedCoProcessFunction[Long, CustomerReport, SensorAlert, (CustomerReport, SensorAlert)]#Context,
                               out: Collector[(CustomerReport, SensorAlert)]): Unit = {
    // access the state value
    if (customerReport.value == "BROKEN_DOWN" || customerReport.value == "DISSATISFIED") {
      latestComplaint.update(customerReport)
      val latestAlertSeen = Option(latestAlert.value)

      latestAlertSeen.foreach {
        alert =>
          if (Math.abs(alert.timestamp - customerReport.timestamp) < Time.hours(2).toMilliseconds) {
            out.collect((customerReport, alert))
          }
      }

    }
  }

  override def processElement2(sensorAlert: SensorAlert,
                               ctx: KeyedCoProcessFunction[Long, CustomerReport, SensorAlert, (CustomerReport, SensorAlert)]#Context,
                               out: Collector[(CustomerReport, SensorAlert)]): Unit = {
    latestAlert.update(sensorAlert)

    val latestComplaintSeen = Option(latestComplaint.value)

    latestComplaintSeen.foreach {
      complaint =>
        if (Math.abs(sensorAlert.timestamp - complaint.timestamp) < Time.hours(2).toMilliseconds) {
          out.collect((complaint, sensorAlert))
        }
    }
  }
}