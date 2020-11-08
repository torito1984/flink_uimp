package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.source.SensorMeasurementSource
import org.apache.flink.streaming.api.scala._

/**
 * Ejercicio 5: Alertas para valvulas con temperaturas altas
 *
 * Issue: Nos reportan que  es necesario saber que valvulas tienen una temperatura mayor a 550 grados, puesto que esto
 * puede ser peligroso. Esta alerta seria menor, de nivel 2.
 *
 * Solución: Filtrar aquellas medidas que muestran un valor mayor a 550 grados y reportar un valor de alerta
 */
object HeatAlert extends JobUtils {

  def main(args: Array[String]) {
    val measurements = env.addSource(new SensorMeasurementSource(100000))

    val criticalAlerts = measurements
      // Filtramos los valores muy grandes
      .filter(m => m.value > 550)
      // Si encontramos alguno reportamos una alerta
      .map(m =>
        SensorAlert(sensorId = m.sensorId,
          units = m.units,
          value = m.value,
          timestamp = m.timestamp,
          level = 2)
      )

    criticalAlerts.print()

    env.execute()
  }
}
