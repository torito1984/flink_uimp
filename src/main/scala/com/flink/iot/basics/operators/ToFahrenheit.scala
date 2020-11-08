package com.flink.iot.basics.operators

import com.flink.iot.basics.source.SensorMeasurementSource
import org.apache.flink.streaming.api.scala._

/**
 * Ejercicio 3: Transformar temperatura a Fahrenheit
 *
 * Issue: Nos informan que los sistemas de monitorizacion de materiales solo funcionan con temperaturas en Fahrenheit, sin embargo el
 * sistema IOT esta reportando en Celsius. Es preciso hacer la correccion antes de reportar valores.
 *
 * Solución: utilizar operadores de Flink
 */
object ToFahrenheit extends JobUtils {

  def main(args: Array[String]) {

    val measurements = env.addSource(new SensorMeasurementSource(100000))

    // map toma cada valor del stream y le aplica una modificacion. El stream resultado en este ejemplo tiene las
    // temperaturas en la unidad correcta
    val farenheit = measurements.map(
      m => m.copy(value = m.value * 1.8 + 32,
        units = "FAHRENHEIT")
    )

    farenheit.print()

    env.execute()
  }
}
