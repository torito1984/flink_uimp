package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.SensorStates._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.source.{SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Ejercicio 13: Temperatura en cada estado
 *
 * Issue: Nos informan que las alarmas por temperatura deberian tener en cuenta el estado en el que se encontraba la valvula
 * en torno al momento de tomar la medicion. Las temperaturas de alarma son diferentes dependiendo del estado
 *  - OPEN-> mas de 500 grados
 *  - CLOSED -> mas de 550
 *  - OBSTRUCTED -> notificar alerta independientemente de la temperatura
 *
 * Las alarmas de temperatura son de nivel 2, las alarmas de OBSTRUCCION son de nivel 3.
 *
 * Solucion: Juntamos las medidas de estado de valvula y temperatura dentro de la misma ventana de 200 ms. Para cada
 * par de temperatura y estado, si encontramos un par de medidas que cumplan el criterio, emitimos una alerta.
 *
 * Separamos las alarmas de temperatura de aquellas que provienen de una obstruccion. De este modo podemos tratarlas
 * independientemente, incluido el darles un nivel de alarma diferente.
 *
 */
object StateTemperatureTracking extends JobUtils {

  def main(args: Array[String]) {
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

    val valveState = env.addSource(new ValveStateSource(10000, 0.01))
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

    // Join the two streams
    val fullState = measurements.join(valveState)
      .where(_.sensorId)
      .equalTo(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
      .apply(new FlatJoinFunction[SensorMeasurement, ValveState, (SensorMeasurement, ValveState)]() {
        override def join(sensorMeasurement: SensorMeasurement, valveState: ValveState,
                          out: Collector[(SensorMeasurement, ValveState)]): Unit = {
          if ((sensorMeasurement.timestamp > valveState.timestamp) &&
            (sensorMeasurement.timestamp - valveState.timestamp) < 20) {
            out.collect((sensorMeasurement, valveState))
          }
        }
      })

    // De cara a tratar las alarmas de distinta naturaleza de manera diferente, las vamos a tratar como side
    // outputs diferentes
    val outputOpenClosed = OutputTag[(SensorMeasurement, ValveState)]("open-closed")
    val outputObstructed = OutputTag[(SensorMeasurement, ValveState)]("obstructed")

    val alertsStream = fullState
      .filter(m => (m._1.value > 500 && m._2.value == SensorOpen()) ||
        (m._1.value > 550 && m._2.value == SensorClose()) ||
        (m._2.value == SensorObstructed()))
      .process(new ProcessFunction[(SensorMeasurement, ValveState), (SensorMeasurement, ValveState)]() {

        override def processElement(value: (SensorMeasurement, ValveState),
                                    ctx: ProcessFunction[(SensorMeasurement, ValveState), (SensorMeasurement, ValveState)]#Context,
                                    out: Collector[(SensorMeasurement, ValveState)]): Unit = {
          value match {
            case (_, ValveState(_, _, SensorOpen())) =>
              ctx.output(outputOpenClosed, value)
            case (_, ValveState(_, _, SensorClose())) =>
              ctx.output(outputOpenClosed, value)
            case _ => ctx.output(outputObstructed, value)
          }
        }
      })

    // Aqui caputuramos las alarmas de temperatura y emitimos de la manera indicada en la especificacion
    alertsStream.getSideOutput(outputOpenClosed).map(m =>
      SensorAlert(sensorId = m._1.sensorId,
        units = "MALFUNCTIONING TEMPERATURE",
        value = m._1.value,
        timestamp = m._1.timestamp,
        level = 2) // El nivel de alarma de temperatura es 2
    ).print()

    // Aqui caputuramos las alarmas de obstuccion y emitimos de la manera indicada en la especificacion
    alertsStream.getSideOutput(outputOpenClosed).map(m =>
      SensorAlert(sensorId = m._1.sensorId,
        units = "MALFUNCTIONING OBSTRUCTED",
        value = m._1.value,
        timestamp = m._1.timestamp,
        level = 3) // El nivel de alarma de obstruccion es 3
    ).print()

    env.execute()
  }
}