package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.source.SensorMeasurementSource
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
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
 * NOTA: En esta solucion utilizamos tiempo de evento para la construccion de ventanas. Debido a esto, las medidas
 * pueden llegar fuera de orden. En la generacion de watermarks, le damos un margen de 3.5 segundos para late events.
 * Este tiempo de margen tiene que ser determinado por experimentacion para cada caso de uso.
 */
object MATemperatureEventTime extends JobUtils {

  def main(args: Array[String]) {
    // Vamos a utilizar tiempo de evento para que las medidas reportadas coincidan con el tiempo percibido por el usuario
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val measurements = env.addSource(new SensorMeasurementSource(100000))

    // Asignamos un watermark de progreso al stream de medidas del sensor.
    val withWatermarks = measurements
      .assignTimestampsAndWatermarks(new WatermarkStrategy[SensorMeasurement] {
      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[SensorMeasurement] = {
        (element: SensorMeasurement, _: Long) => element.timestamp
      }

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[SensorMeasurement] = {
        new WatermarkGenerator[SensorMeasurement]() {
          // Este el el tiempo maximo que permitimos para que una medida tardia se considere dentro de su ventana
          val maxOutOfOrderness: Long = 3500L // 3.5 seconds
          // El ultimo timestamp observado
          var currentMaxTimestamp: Long = _

          override def onEvent(event: SensorMeasurement, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            currentMaxTimestamp = math.max(eventTimestamp, currentMaxTimestamp)
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
          }
        }
      }
    })

    // Que ocurre si no asigno watemarks al stream? Puedes probarlo tomando measurements como el stream base de este
    // job. Deberias observar que, sin watermarks, el tiempo no pasa para el job, las ventanas no se ejecutan nunca,
    // y los valores se acumulan en memoria sin ser reportados nunca. Con tiempo de evento es necesario asignar watermarks
    // para que cualquier mecanismo de ventanas temporales funcione.
    //val useMeasurements = measurements
    val useMeasurements = withWatermarks

    // Este calculo es el mismo que en el ejercicio anterior. La unica diferencia es que el tiempo reportado y la
    // asignacion a ventanas temporales corresponde al tiempo de evento reportado por los sensores en origen
    val meanRegistered = useMeasurements
      .keyBy(_.sensorId)
      .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
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
      // Observamos solo un sensor por simplicidad
      .filter(m => m._1 == 11080)

    meanRegistered.print()

    env.execute()
  }
}
