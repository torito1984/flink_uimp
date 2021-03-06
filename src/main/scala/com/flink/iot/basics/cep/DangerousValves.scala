package com.flink.iot.basics.cep

import java.util

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.SensorStates._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.operators.JobUtils
import com.flink.iot.basics.source.{SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * Ejercicio 17: Errores complejos
 *
 * Issue: Nos solicitan que detectemos patrones de fallo mas complejos. En concreto, si una valvula se obstruye
 * (notifica OBSTRUCTED mas de 5 veces seguidas) y posteriormente la temperatura sube, se puede determinar que
 * la valvula esta en riesgo de rotura. Este tipo de alerta es de nivel 5.
 *
 * Solucion: utilizando la libreria de Complex Event Processing de Flink, podemos detectar esta secuencia de eventos
 * del stream conjunto de temperaturas y estados de valvulas. Si este patron se detecta para algun sensor, se notifica
 * la alerta de un nivel superior.
 *
 */
object DangerousValves extends JobUtils {

  // Stream de medidas de temperatura
  private def getMeasurementStream(env: StreamExecutionEnvironment): DataStream[(SensorMeasurement, ValveState)] = {
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

    // Stream de medidas de estado de valvulas
    val valveState = env.addSource(new ValveStateSource(10000, 0.3))
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

    // Juntamos medidas de temperatura y valvula que esten proximas en el tiempo
    measurements.join(valveState)
      .where(_.sensorId)
      .equalTo(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(new FlatJoinFunction[SensorMeasurement, ValveState, (SensorMeasurement, ValveState)]() {
        override def join(sensorMeasurement: SensorMeasurement, valveState: ValveState, out: Collector[(SensorMeasurement, ValveState)]): Unit = {
          if ((sensorMeasurement.timestamp > valveState.timestamp) && (sensorMeasurement.timestamp - valveState.timestamp) < 200) {
            out.collect((sensorMeasurement, valveState))
          }
        }
      })
  }

  def main(args: Array[String]) {

    val fullState = getMeasurementStream(env)

    /**
     * Definimos el patron de error como:
     *  - comenzamos con un comportamiento normal
     *  - detectamos 5 medidas o mas en estado OBSTRUCTED
     *  - el patron temina con una deteccion de aumento de temperatura
     */

    val pattern = Pattern
      .begin[(SensorMeasurement, ValveState)]("normal")
      .where(e => (e._2.value == SensorOpen()) || (e._2.value == SensorClose()))
      .next("closure").timesOrMore(5).greedy
      .where(e => (e._2.value == SensorClose()) || (e._2.value == SensorObstructed()))
      .followedBy("heating")
      .where(e => e._1.value > 500)


    // Aplicamos el CEP al stream de medidas de sensor
    val patternStream = CEP.pattern(
      // Agrupamos el stream para cada sensor antes de aplicar el patron
      fullState.keyBy(_._1.sensorId), pattern)

    // Una vez detectamos un evento que cuadra con el patron notificamos una alerta con el nivel adecaudo
    val complexAlert = patternStream.process(
      new PatternProcessFunction[(SensorMeasurement, ValveState), SensorAlert]() {

        override def processMatch(`match`: util.Map[String, util.List[(SensorMeasurement, ValveState)]],
                                  ctx: PatternProcessFunction.Context, out: Collector[SensorAlert]): Unit = {
          val pattern = `match`.asScala
          val alert = SensorAlert(sensorId = pattern("normal").asScala.head._1.sensorId,
            units = s"MALFUNCTIONING NOT OPENING TIMES ${pattern("closure").asScala.size}",
            value = pattern("heating").asScala.head._1.value,
            timestamp = pattern("heating").asScala.head._1.timestamp,
            level = 5)
          out.collect(alert)
        }
      })

    complexAlert.print()

    env.execute()
  }
}
