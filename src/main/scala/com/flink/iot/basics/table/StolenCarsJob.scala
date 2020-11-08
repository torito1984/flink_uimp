package com.flink.iot.basics.table

import java.util.concurrent.TimeUnit

import com.flink.iot.basics.async.perevent.AsyncEnrichmentFunction
import com.flink.iot.basics.db.VehicleOwnerDataClient.MaxParallelism
import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.entities.VehicleEntities.{EnrichedVehicleState, VehicleState}
import com.flink.iot.basics.source.{SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 * Ejercicio 18: Vehiculos robados
 *
 * Issue: Nos notifican que la policia quiere acceso a nuestro stream de actividad de vehiculos con el objetivo de poder
 * monitorizar la actividad de vehiculos privados. Debido a cuestiones de LOPD, no podemos compartir la actividad de todos
 * nuestros usuarios, asi que como solucion nos proporcionan un stream de notificaciones de vehiculos robados. Si
 * detectamos que ese vehiculo esta siendo utilizado, lo notificamos a la policia.
 *
 * Solucion: En esta solucion vamos a utilizar el Table API. Para ellos cargamos el stream enriquecido por el CRM
 * (se ha introducido el reporte de robado al CRM) y posteriormente simplemente se notifica la actividad de automobiles
 * que han sido notificados como robados.
 *
 */
object StolenCarsJob {

  // Esta funcion junta el stream de actividad de vehiculos y lo enriquecemos con la infomracion del CRM
  private def getVehicleState(env: StreamExecutionEnvironment): DataStream[EnrichedVehicleState] = {
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

    // Connect all the streams to get a potential state of the vehicle
    val vehicleState = measurements.connect(valveState)
      .map(
        (m: SensorMeasurement) => VehicleState(m.sensorId / 100, 1, m.timestamp),
        (v: ValveState) => VehicleState(v.sensorId / 100, 1, v.timestamp)
      )


    val enrichedMeasurements = AsyncDataStream.unorderedWait(
      vehicleState,
      new AsyncEnrichmentFunction(), 50, TimeUnit.MILLISECONDS, MaxParallelism)

    enrichedMeasurements
  }

  def main(args: Array[String]) {
    val fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)

    // Registramos el stream enriquecido de actividad como una tabla dinamica
    fsTableEnv.createTemporaryView("vehicles_moving", getVehicleState(fsEnv))

    // The schema is inferred automatically by Flink
    //fsTableEnv.scan("vehicles_moving").printSchema();
    // Filtramos la tabla dinamica con solo aquellos vehiculos en actividad que se han reportado como robados
    val stolenCarCases = fsTableEnv.from("vehicles_moving")
      .filter($"owner".get("reportedStolen") === true)
      .select($"state".get("vehicleId"), $"state".get("timestamp"), $"owner")

    // Convertimos la tabla dinamica de nuevo a un stream de Flink habitual y notificamos
    fsTableEnv.toAppendStream[Row](stolenCarCases).print()

    fsEnv.execute()
  }
}
