package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.source.{SecondarySensorMeasurementSource, SensorMeasurementSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Ejercicio 11: Sensor secundario por seguirdad
 *
 * Issue: Nos comunican que, en los nuevos modelos, las valvulas llevan 2 sensores por seguridad. Nos ordenan comparar
 * las medidas recibidas por ambos sensores. En el caso que la desviacion sea mayor a 20 grados, reportar un error
 * de medida.
 *
 * Solucion: ingestar los dos streams de datos, juntarlos por el id de sensor y comparar las medidas recibidas en torno
 * al mismo momento en el tiempo. Si la desviacion es muy grande, reportar un error.
 *
 * NOTA: Esta solucion es correcta y mas eficiente, gracias a la utilizacion de un agregador antes de aplicar la funcion
 * de ejecucion de ventana.
 *
 */
object EfficientSecondarySensor extends JobUtils {
  val MaxTolerance = 20

  def main(args: Array[String]) {

    val measurements = env.addSource(new SensorMeasurementSource(100000))
    val secondaryMeasurements = env.addSource(new SecondarySensorMeasurementSource(100000))

    // Map to a common API
    val primaryValues = measurements.map(m => SensorValue(sensorId = m.sensorId,
      timestamp = m.timestamp,
      value = m.value,
      units = m.units,
      `type` = "PRIMARY"))

    val secondaryValues = secondaryMeasurements.map(m => SensorValue(sensorId = m.sensorId,
      timestamp = m.timestamp,
      value = m.value,
      units = m.units,
      `type` = "SECONDARY"))

    // Union the streams and assign watermarks
    val allValues = primaryValues.union(secondaryValues)
      // le asignamos al stream un watermark
      .assignTimestampsAndWatermarks(new WatermarkStrategy[SensorValue] {
        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorValue] = {
          (element: SensorValue, _: Long) => element.timestamp
        }

        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorValue] = {
          new WatermarkGenerator[SensorValue]() {
            // Este el el tiempo maximo que permitimos para que una medida tardia se considere dentro de su ventana
            val maxOutOfOrderness: Long = 3500L // 3.5 seconds
            // El ultimo timestamp observado
            var currentMaxTimestamp: Long = _

            override def onEvent(event: SensorValue, eventTimestamp: Long, output: WatermarkOutput): Unit = {
              currentMaxTimestamp = math.max(eventTimestamp, currentMaxTimestamp)
            }

            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
              output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
            }
          }
        }
      })

    val sensorAlerts = allValues
      .keyBy(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // En esta solucion, usamos un agregador que mantiene el total para ambos sensores actualizado hasta el momento
      // de ejecutar la ventana. Ver el codigo de DoubleSensorAverageAggregate para ver como se realiza el calculo
      .aggregate(new DoubleSensorAverageAggregate()
        , new ProcessWindowFunction[(Double, Double, Double, Double), SensorAlert, Long, TimeWindow]() {
          override def process(key: Long, context: Context,
                               elements: Iterable[(Double, Double, Double, Double)],
                               out: Collector[SensorAlert]): Unit = {

            val sensorId = key
            val agg = elements.iterator.next
            if (agg._1 > 0 && agg._3 > 0) {
              val sensorDiff = Math.abs(agg._2 / agg._1 - agg._4 / agg._3)
              if (sensorDiff > MaxTolerance) {
                val alert = SensorAlert(
                  timestamp = context.window.getEnd,
                  sensorId = sensorId,
                  units = "SENSOR DIFF CELSIUS",
                  value = sensorDiff,
                  level = 2)
                out.collect(alert)
              }
            }
          }
        })

    sensorAlerts.print()
    env.execute()
  }
}
