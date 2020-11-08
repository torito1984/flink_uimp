package com.flink.iot.basics.operators

import org.apache.flink.streaming.api.scala._
import com.flink.iot.basics.source.SensorMeasurementSource

/**
 * Ejercicio 2: Lectura de señales procedentes del sensor de temperatura.
 *
 * En este ejercicio unicamente nos conectamos a la fuente de datos IOT de todos los sensores de temperatura desplegados
 * e imprimimos sus valores para observar la estructura que tienen. La estructura del programa es exactamente igual
 * a la del ejercicio 1 (ver FlinkHelloWorld)
 */
object BasicPrintJob extends JobUtils {

	def main(args: Array[String]) {

		// Leer fuente de datos
		val measurements = env.addSource(new SensorMeasurementSource(100000))

		// Imprimir
		measurements.print()

		// Ejecutar
		env.execute()
	}
}
