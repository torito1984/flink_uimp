package com.flink.iot.basics.source

import java.util.SplittableRandom
import com.flink.iot.basics.entities.SensorEntities._

class SensorMeasurementSource(val maxRecordsPerSecond: Int)
	extends BaseGenerator[SensorMeasurement](maxRecordsPerSecond) {

	val SensorCount: Long = 100000L

	override protected def randomEvent(rnd: SplittableRandom, id: Long): SensorMeasurement = {
		SensorMeasurement(sensorId = rnd.nextLong(SensorCount),
			timestamp = System.currentTimeMillis() - rnd.nextLong(1000L),
			value = rnd.nextDouble(600),
			units = "CELSIUS")
	}
}
