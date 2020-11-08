package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import com.flink.iot.basics.entities.ValveEntities._
import com.flink.iot.basics.entities.VehicleEntities.VehicleState
import com.flink.iot.basics.source.{SensorMeasurementSource, ValveStateSource}
import org.apache.flink.api.common.eventtime._
import org.apache.flink.streaming.api.scala._

/**
 * Ejercicio 14: Monitorizar uso de los vehiculos
 *
 * Issue: Nos piden si seria posible aprovechar las medidas de todos los sensores del coche para saber patrones de uso
 * de los automobiles. Se considera que si los sensores esta midiendo actividad, significa que el automobil esta en uso.
 * Debido a que distintos sensores notifican en distintos momentos del tiempo, seria util pode utilizar todas las medidas
 * en conjunto para tener una traza lo mas fiable posible.
 *
 * Solucion: conectamos todos los streams con los que contamos en un colo stream y los transformamos a un api comun
 * que notifique el uso del automobil.
 *
 */
object VehicleStateTracking extends JobUtils {

  def main(args: Array[String]) {

    // Ingestamos todos los steam disponibles

    // Medidas de temperatura
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

    // Medidas de estado de las valvulas
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

    // Conectamos todos los streams en uno unico
    val vehicleState = measurements.connect(valveState)
      // Comap nos permite tratar cada stream de manera diferente. Utilizamos esta capacidad para mapear ambos
      // streams a una API de actividad comun
      .map(
        (m: SensorMeasurement) => VehicleState(m.sensorId / 100, 1, m.timestamp),
        (v: ValveState) => VehicleState(v.sensorId / 100, 1, v.timestamp)
      )
    vehicleState.print()

    env.execute()
  }
}