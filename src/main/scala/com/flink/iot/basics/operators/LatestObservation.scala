package com.flink.iot.basics.operators

import org.apache.flink.streaming.api.scala._
import com.flink.iot.basics.source.SensorMeasurementSource

/**
 * Ejercicio 6: Ultima medida tomada para cada coche.
 *
 * Issue: Nos reportan que seria util saber en todo momento cuando fue la ultima medida observada para cada coche. Esto
 * podria ayudar a saber si un coche no se usa durante mucho tiempo y notificar al usuario que esto puede traer problemas
 * en el futuro.
 *
 * Solución: Monitorizar el ultimo momento que hemos recibido medidas para cada automobil.
 *
 */
object LatestObservation extends JobUtils {

  def main(args: Array[String]) {
    val measurements = env.addSource(new SensorMeasurementSource(100000))

    val latestObservation = measurements
      // Agrupamos las medidas por sensor
      .keyBy(_.sensorId)
      // Monitorizamos la mayor fecha de medida recibida
      .maxBy("timestamp")
      // Observamos un sensor en concreto por simplicidad (TODO: quitar en produccion)
      .filter(m => m.sensorId == 11080)

    latestObservation.print()

    env.execute()
  }
}
