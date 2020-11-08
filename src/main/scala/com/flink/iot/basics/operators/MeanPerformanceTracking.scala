package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.SensorStates._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.source.{SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Ejercicio 12: Historico de perfomance de cada valvula
 *
 * Issue: Nos piden que acumulemos tanto la temperatura media, como el porcentaje de cada estado en cada valvula.
 * Durante el funcionamiento, las valvulas pueden estar en estado OPEN, CLOSE u OBSTRUCTED. Se necesita saber que el porcentaje
 * de OPEN y CLOSE esta en equilibrio - mostrando un funcionamiento normal - y que el porcentaje del tiempo en OBSTRUCTED es
 * nulo o muy bajo.
 *
 */
object MeanPerformanceTracking extends JobUtils {

  def main(args: Array[String]) {

    // Ingestamos las medidas de temperatura
    val measurements = env.addSource(new SensorMeasurementSource(100000))
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

    // Ingestamos las medidas de estado de valvula
    val valveState = env.addSource(new ValveStateSource(100000, 0.01))
      .assignTimestampsAndWatermarks(new WatermarkStrategy[ValveState] {
        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[ValveState] = {
          (element: ValveState, _: Long) => element.timestamp
        }

        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[ValveState] = {
          new WatermarkGenerator[ValveState]() {
            // Este el el tiempo maximo que permitimos para que una medida tardia se considere dentro de su ventana
            val maxOutOfOrderness: Long = 3500L // 3.5 seconds
            // El ultimo timestamp observado
            var currentMaxTimestamp: Long = _

            override def onEvent(event: ValveState, eventTimestamp: Long, output: WatermarkOutput): Unit = {
              currentMaxTimestamp = math.max(eventTimestamp, currentMaxTimestamp)
            }

            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
              output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
            }
          }
        }
      })

    // Juntamos ambos streams flyyendo en paralelo
    val meanPerformance = measurements.coGroup(valveState)
      // Agrupamos las medidas para cada sensor
      .where(_.sensorId)
      .equalTo(_.sensorId)
      // Revisamos en ventanas de 25 segundos
      .window(TumblingEventTimeWindows.of(Time.seconds(25)))
      // Aplicando una funcion de cogroup
      .apply {
        /**
         * Como se puede observar, coGroup nos da dos iteradores independientes para los elementos que entran
         * dentro de la ventana temporal para ambos streams
         */
        (measurements: Iterator[SensorMeasurement], states: Iterator[ValveState], out: Collector[MeanValveBehavior]) =>
          // Calculo de estado medio
          val (total: Int, open: Double, closed: Double, obstructed: Double) =
            states.foldLeft((0, 0.0, 0.0, 0.0)) {
              (acc, state) =>
                state.value match {
                  case SensorOpen() => (acc._1 + 1, acc._2 + 1, acc._3, acc._4)
                  case SensorClose() => (acc._1 + 1, acc._2, acc._3 + 1, acc._4)
                  case SensorObstructed() => (acc._1 + 1, acc._2, acc._3, acc._4 + 1)
                }

            }

          // Calculo de temperatura media
          val (totalTmp, temperatureAcc) =
            measurements.foldLeft((0, 0.0)) {
              (acc, temperature) =>
                (acc._1 + 1, acc._2 + temperature.value)
            }

          // Emitir el performance medio
          if (total > 0 && totalTmp > 0)
            out.collect(MeanValveBehavior(open / total, closed / total,
              obstructed / total, temperatureAcc / totalTmp))
      }

    meanPerformance.print()

    env.execute()
  }
}