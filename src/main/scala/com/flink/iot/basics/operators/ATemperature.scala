package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.source.SensorMeasurementSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
 * Ejercicio 8: Calcular la temperatura media de uso
 *
 * Issue: nos piden reportar la temperatura media de uso de cada valvula.
 *
 * Solucion: mantener un agregado continuo de valores y numero de medidas. Cada vez que llegue un elemento, reportar
 * un nuevo valor medio. En caso de que este calculo sea mucha carga para cada medida recibida, se puede reducir el
 * numero de reportes por medida recibida.
 */
object ATemperature extends JobUtils {

  def main(args: Array[String]) {
    val measurements = env.addSource(new SensorMeasurementSource(100000))

    val meanRegistered = measurements
      .keyBy(_.sensorId)
      // Agrupamos todas las medidas en una ventana
      .windowAll(GlobalWindows.create())
      // Emitimos un valor para la ventana de medidas cada vez que llegue un elemento
      .trigger(CountTrigger.of(1))
      // Agregamos el total y el valor acumulado global
      .aggregate(new AggregateFunction[SensorMeasurement, MAValue, MAValue]() {

        override def createAccumulator(): MAValue = MAValue()

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
      }
        // Cada vez que se ejecute la ventana, procesamos el acumulado para generar la media global
        , new ProcessAllWindowFunction[MAValue, (Long, Long, Double), GlobalWindow]() {
          override def process(context: Context, elements: Iterable[MAValue], out: Collector[(Long, Long, Double)]): Unit = {
            val value = elements.iterator.next()
            out.collect((value.sensorId, value.timestamp, value.cum / value.count))
          }
        })
      // Filtramos el valor de un sensor por simplicidad (quitar en produccion)
      .filter(m => m._1 == 11080)

    meanRegistered.print()

    env.execute()
  }
}
