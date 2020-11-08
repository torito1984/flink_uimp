package com.flink.iot.basics.queryable

import java.util

import com.flink.iot.basics.entities.CustomerEntities._
import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.SensorStates._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.operators.JobUtils
import com.flink.iot.basics.source.{ClientCommunicationSource, SensorMeasurementSource, ValveStateSource}
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
 * Ejercicio 20: Consulta de ultima alarma y ultima queja registrada
 *
 * Issue: Desde el departamento de atencion al cliente nos piden poder consultar la ultima queja de propietario
 * o ultima alarma de telemetria registrada por cada uno de los vehiculos.
 *
 * Solucion: Partiendo de la solucion del ejercicio 16, ya tenemos resuelto este caso de uso. El unico cambio
 * que es necesario introducir el el ProcessFuncion es hacer el estado para cada vehiculo abierto a consulta desde
 * fuera del pipeline. Para ello utilizamos la capacidad de queryable state de Flink >1.9
 *
 */
object QueryableCustomerServiceJob extends JobUtils {

  // Stream de medidas
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

    // Join the two streams
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

  // Stream de alertas
  private def getAlerts(env: StreamExecutionEnvironment): DataStream[SensorAlert] = {
    val fullState = getMeasurementStream(env)

    val pattern = Pattern
      .begin[(SensorMeasurement, ValveState)]("normal")
      .where(e => (e._2.value == SensorOpen()) || (e._2.value == SensorClose()))
      .next("closure").timesOrMore(5).greedy
      .where(e => (e._2.value == SensorClose()) || (e._2.value == SensorObstructed()))
      .followedBy("heating")
      .where(e => e._1.value > 500)

    val patternStream = CEP.pattern(fullState.keyBy(_._1.sensorId), pattern)

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

    complexAlert
  }

  // Stream de reportes de propietarios
  private def getCustomerReports(env: StreamExecutionEnvironment): DataStream[CustomerReport] = {
    val reports = env.addSource(new ClientCommunicationSource(1000, 0.05))
      .assignTimestampsAndWatermarks(new WatermarkStrategy[CustomerReport] {
        override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[CustomerReport] = {
          (element: CustomerReport, _: Long) => element.timestamp
        }

        override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[CustomerReport] = {
          new WatermarkGenerator[CustomerReport]() {
            // Este el el tiempo maximo que permitimos para que una medida tardia se considere dentro de su ventana
            val maxOutOfOrderness: Long = 3500L // 3.5 seconds
            // El ultimo timestamp observado
            var currentMaxTimestamp: Long = _

            override def onEvent(event: CustomerReport, eventTimestamp: Long, output: WatermarkOutput): Unit = {
              currentMaxTimestamp = math.max(eventTimestamp, currentMaxTimestamp)
            }

            override def onPeriodicEmit(output: WatermarkOutput): Unit = {
              output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
            }
          }
        }
      })

    reports
  }

  def main(args: Array[String]) {

    val customerReports = getCustomerReports(env).keyBy(_.vehicleId)
    val alerts = getAlerts(env).keyBy(_.sensorId / 100)

    // permitimos que el estado del pipeline sea consultable desde el exterior.
    // Ver la funcion open de QueryablePotentialCauseProcess y como consultar desde un programa externo en QueryablePotentialCauseProcess
    val potentialCauses = customerReports.connect(alerts)
      .process(new QueryablePotentialCauseProcess())

    potentialCauses.print()
    env.execute()
  }
}
