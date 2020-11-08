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
 * Ejercicio 10: Sensor secundario por seguirdad
 *
 * Issue: Nos comunican que, en los nuevos modelos, las valvulas llevan 2 sensores por seguridad. Nos ordenan comparar
 * las medidas recibidas por ambos sensores. En el caso que la desviacion sea mayor a 20 grados, reportar un error
 * de medida.
 *
 * Solucion: ingestar los dos streams de datos, juntarlos por el id de sensor y comparar las medidas recibidas en torno
 * al mismo momento en el tiempo. Si la desviacion es muy grande, reportar un error.
 *
 * NOTA: Esta solucion es correcta, pero se puede hacer mas eficiente. Recordar que al utilizar el API de bajo nivel
 * process, se acumulan todos los valores de cada ventana hasta que esta se ejecuta. Ver el siguiente ejercicio para
 * una solucion mas eficiente.
 *
 */
object SecondarySensor extends JobUtils {
  val MaxTolerance = 20

  def main(args: Array[String]) {

    // Ingestamos el stream de medidas primario
    val measurements = env.addSource(new SensorMeasurementSource(100000))
    // Ingestamos el stream de medidas secundario
    val secondaryMeasurements = env.addSource(new SecondarySensorMeasurementSource(100000))

    // Convertimos las medidas de ambos sensores a un mismo API, de este modo podremos unir ambas medidas en el
    // mismo stream. Marcamos si la medida proviene de un sensor primario o secundario
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

    // Unimos ambos streams. De este modo, es transparente la procedencia de la medida
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
      // Agrupamos las medidas recibidas para cada sensor en ventamas de 10 segundos
      .keyBy(_.sensorId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // para cada ventana, calculamos la temperatura media del sensor primario y secundario, en el caso de encontrar
      // una desviacion significativa, reportamos una alerta
      .process(new ProcessWindowFunction[SensorValue, SensorAlert, Long, TimeWindow]() {
        override def process(key: Long, context: Context, elements: Iterable[SensorValue], out: Collector[SensorAlert]): Unit = {

          // sensorId, countPrimary, sumPrimary, countSecondary, sumSecondary
          val (sensorId, countPrimary, sumPrimary, countSecondary, sumSecondary) = elements.
            foldLeft((0L, 0.0, 0.0, 0.0, 0.0)) {
              (acc, sensorValue) =>
                sensorValue match {
                  case SensorValue("PRIMARY", _, sensorId, _, value) =>
                    (sensorId, acc._2 + 1, acc._3 + value, acc._4, acc._5)
                  case SensorValue(_, _, sensorId, _, value) =>
                    // SECONDARY
                    (sensorId, acc._2, acc._3, acc._4 + 1, acc._5 + value)
                }
            }

          val sensorDiff = Math.abs(sumPrimary / countPrimary - sumSecondary / countSecondary)

          // Si la diferencia es muy grande, reportar una alerta
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
      })

    sensorAlerts.print()
    env.execute()
  }
}
