package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.source.SensorMeasurementSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Ejercicio 9: Media mobil de la temperatura de uso
 *
 * Issue: Nos piden acumular la temperatura media de uso cada 10 segundos, con el objetivo de observar los distintos
 * patrones de utilizacion de la valvulas.
 *
 * Solucion: Construir ventanas de tiempo y calcular la media dentro de cada ventana.
 *
 * NOTA: En esta solucion utilizamos el tiempo de proceso, es decir, el momento en el que cada medida es procesada por
 * Flink. Si las medidas no llegan de inmediato al sistema (cosa poco probable puesto que los sensores emiten en remoto)
 * el tiempo reportado por Flink para cada media estara desalineado con el tiempo de uso que el usuario percibe de su
 * coche. Ver siguiente ejercicio para un manejo correcto del tiempo de evento.
 */
object MATemperatureProcessingTime extends JobUtils {

  def main(args: Array[String]) {
    val measurements = env.addSource(new SensorMeasurementSource(100000))

    // utilizamos tiempo de proceso para la construccion de las ventanas
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val meanRegistered = measurements
      .keyBy(_.sensorId)
      // Construimos ventanas cada 10 segundos, moviendose cada 5 segundos
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
      // Agregamos el valor total para cada ventana
      .aggregate(new AggregateFunction[SensorMeasurement, MAValue, MAValue]() {
        override def createAccumulator(): MAValue = {
          MAValue()
        }

        override def add(value: SensorMeasurement, accumulator: MAValue): MAValue = {
          MAValue(
            sensorId = value.sensorId,
            timestamp = if (value.timestamp > accumulator.timestamp) value.timestamp else accumulator.timestamp,
            count = accumulator.count + 1,
            cum = accumulator.cum + value.value
          )
        }

        override def getResult(accumulator: MAValue): MAValue = {
          accumulator
        }

        override def merge(a: MAValue, b: MAValue): MAValue = {
          MAValue(sensorId = a.sensorId,
            timestamp = if (a.timestamp < b.timestamp) b.timestamp else a.timestamp,
            count = a.count + b.count,
            cum = a.cum + b.cum
          )
        }
      }, new ProcessWindowFunction[MAValue, (Long, Long, Double), Long, TimeWindow]() {
        override def process(key: Long, context: Context, elements: Iterable[MAValue], out: Collector[(Long, Long, Double)]): Unit = {
          val value = elements.iterator.next()
          out.collect((value.sensorId, value.timestamp, value.cum / value.count))
        }
      })
      // filtramos solo un sensor por simplicidad de observacion (quitar en produccion)
      .filter(m => m._1 == 11080)

    meanRegistered.print()

    env.execute()
  }
}
