package com.flink.iot.basics.operators

import org.apache.flink.streaming.api.scala._
import com.flink.iot.basics.source.SensorMeasurementSource

/**
 * Ejercicio 7 (version 2): Maxima temperatura registrada para cada valvula.
 *
 * Issue: Nos reportan que es necesario registrar cual ha sido la mayor temperatura registrada para cada valvula. Si la
 * temperatura maxima es muy alta, puede afectar al funcionamiento futuro del automobil, incluso aunque la temperatura
 * sea soportada durante un periodo corto de tiempo.
 *
 * Solucion: En este caso resolvemos el mismo caso con la operacion maximo predefinida en Flink
 *
 */
object MaxRegisteredTemperatureAggregation extends JobUtils {

  def main(args: Array[String]) {
    val measurements = env.addSource(new SensorMeasurementSource(100000))

    val maxRegistered = measurements
      .keyBy(_.sensorId)
      .maxBy("value")
      .filter(m => m.sensorId == 11080)

    maxRegistered.print()

    env.execute()
  }
}
