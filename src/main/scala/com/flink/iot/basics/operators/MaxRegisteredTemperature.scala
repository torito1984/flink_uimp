package com.flink.iot.basics.operators

import org.apache.flink.streaming.api.scala._
import com.flink.iot.basics.source.SensorMeasurementSource

/**
 * Ejercicio 7: Maxima temperatura registrada para cada valvula.
 *
 * Issue: Nos reportan que es necesario registrar cual ha sido la mayor temperatura registrada para cada valvula. Si la
 * temperatura maxima es muy alta, puede afectar al funcionamiento futuro del automobil, incluso aunque la temperatura
 * sea soportada durante un periodo corto de tiempo.
 */
object MaxRegisteredTemperature extends JobUtils {

  def main(args: Array[String]) {
    val measurements = env.addSource(new SensorMeasurementSource(100000))

    val maxRegistered = measurements
      // Agrupamos las medidas por sensor
      .keyBy(_.sensorId)
      // Calculamos la temperatura maxima manualmente a traves una operacion de reduccion
      .reduce((a, b) => if (a.value > b.value) a else b)
      // Observar un sensor por simplicidad
      .filter(m => m.sensorId == 11080)

    maxRegistered.print()

    env.execute()
  }
}
