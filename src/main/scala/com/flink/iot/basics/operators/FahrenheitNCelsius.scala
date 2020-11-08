package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.source.SensorMeasurementSource
import org.apache.flink.streaming.api.scala._

/**
 * Ejercicio 4: Necesitamos las medidas tanto en celsius como en Fahrenheit
 *
 * Issue: La solución anterior es correcta, pero nos reportan que las medidas en celsius tambien eran necesarias para
 * los cuadros de mandos.
 *
 * Solución: reportar ambas medidas
 */
object FahrenheitNCelsius extends JobUtils {

  def main(args: Array[String]) {

    val measurements = env.addSource(new SensorMeasurementSource(100000))

    // flatmap nos permite generar 0 o mas elementos para cada elemento de entrada. Gracias a esto podemos reportar
    // en las dos unidades de medida solicitadas
    val farenheit = measurements.flatMap {
      m =>
        List(m, SensorMeasurement(
          sensorId = m.sensorId,
          units = "FAHRENHEIT",
          value = m.value * 1.8 + 32,
          timestamp = m.timestamp))
    }

    farenheit.print()

    env.execute()
  }
}
