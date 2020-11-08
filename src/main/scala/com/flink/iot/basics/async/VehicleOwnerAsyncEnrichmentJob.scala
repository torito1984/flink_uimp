package com.flink.iot.basics.async

import java.util.concurrent.TimeUnit

import com.flink.iot.basics.async.perevent.AsyncEnrichmentFunction
import com.flink.iot.basics.db.VehicleOwnerDataClient.MaxParallelism
import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.entities.VehicleEntities._
import com.flink.iot.basics.operators.JobUtils
import com.flink.iot.basics.source.{SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment, _}

/**
 * Ejercicio 15: Anhadir informacion de propietario
 *
 * Issue: Nos piden que cada medida de actividad de los automobiles vaya acompañada de la informacion del dueño del
 * vehiculo. Esta informacion sera explotada en varios casos de uso futuros. Nos indican que si eliminamos las ultimas
 * 2 cifas significativas del id de sensor, obtenemos el id de vehiculo, que podemos cruzar con el CRM.
 *
 * Solucion: para cada medida nos conectamos a la base de datos de propietarios y encontramos el usuario adecuado. Juntamos
 * ambas fuentes de informacion y publicamos.
 *
 */
object VehicleOwnerAsyncEnrichmentJob extends JobUtils {

  /**
   * Esta funcion genera el stream de actividad de vehiculos, ver ejercicio 14
   */
  private def getVehicleState(env: StreamExecutionEnvironment): DataStream[VehicleState] = {
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
    // Conectamos todos los streams en uno unico
    val vehicleState = measurements.connect(valveState)
      // Comap nos permite tratar cada stream de manera diferente. Utilizamos esta capacidad para mapear ambos
      // streams a una API de actividad comun
      .map(
        (m: SensorMeasurement) => VehicleState(m.sensorId / 100, 1, m.timestamp),
        (v: ValveState) => VehicleState(v.sensorId / 100, 1, v.timestamp)
      )

    vehicleState
  }

  def main(args: Array[String]) {

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Cargamos el stream de propietarios
    val vehicleState = getVehicleState(env)

    // Lo enriquecemos con informacion del CRM. Puesto que la llamda tiene cierto delay, lo hacemos de forma
    // asincrona. Recordad poner siempre un timeout para cada llamada y un maximo de llamadas en vuelo. Sino
    // es posible que el stream se bloquee
    val enrichedMeasurements = AsyncDataStream.unorderedWait(
      vehicleState,
      new AsyncEnrichmentFunction,
      50, TimeUnit.MILLISECONDS, MaxParallelism)

    enrichedMeasurements.print()

    env.execute()
  }
}
