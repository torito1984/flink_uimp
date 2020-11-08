package com.flink.iot.basics.sql

import java.util

import com.flink.iot.basics.entities.CustomerEntities._
import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.SensorStates._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.source.{ClientCommunicationSource, SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * Ejercicio 19: Atencion al cliente
 *
 * Issue: Se introduce un nuevo sistema por el cual los propietarios pueden reportar via una aplicacion mobil
 * si han tenido algun problema con el vehiculo. Tenemos acceso a este stream de notificaciones. El servicio de atencion
 * al cliente nos pide si seria posible tener las ultimas alertas en torno un reporte de DISSATIFFIED o BREAK_DOWN
 * para poder tener una conversacion mejor informada con el propietario.
 *
 * Solucion: conectar el stream de alertas y de notificaciones de usuario y tratar de determinar una potencial causa
 * de error. En esta solucion, conseguimos un resultado similar al del ejercicio 16, pero realizado con Flink SQL
 *
 */
object SQLCustomerServiceJob {

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
    val fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)

    // Registramos el stream de notificaciones de propietarios como tabla dinamica
    fsTableEnv.createTemporaryView("customer_reports", getCustomerReports(fsEnv),
      'vehicleId, 'customerName, 'value.as("report_value"), 'timestamp.rowtime.as("report_time"))
    // Registramos el stream de alertas de telemetria como tabla dinamica
    fsTableEnv.createTemporaryView("telemetry_alerts", getAlerts(fsEnv),
      'sensorId, 'units, 'value.as("alert_value"), 'level, 'timestamp.rowtime.as("alert_time"))

    //fsTableEnv.scan("customer_reports").printSchema()

    // Conectamos cada notificacion con una potencial alerta que causa el mal funcionamiento en torno a 1h antes y despues
    // de la notificacion
    val potentialCauses = fsTableEnv.sqlQuery(
      "SELECT c.vehicleId, c.customerName, c.report_value, c.report_time, a.sensorId, a.units, a.alert_value, a.level " +
        "FROM customer_reports c, telemetry_alerts a " +
        "WHERE c.vehicleId = (a.sensorId/100) " +
        "AND c.report_value IN ('BROKEN_DOWN', 'DISSATISFIED') " +
        "AND a.alert_time BETWEEN c.report_time - INTERVAL '1' HOUR AND c.report_time + INTERVAL '1' HOUR")

    fsTableEnv.toAppendStream[Row](potentialCauses).print()

    fsEnv.execute()
  }
}
