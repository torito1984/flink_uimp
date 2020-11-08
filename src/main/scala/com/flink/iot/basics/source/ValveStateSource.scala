package com.flink.iot.basics.source

import java.util.SplittableRandom
import com.flink.iot.basics.entities.SensorStates._
import com.flink.iot.basics.entities.ValveEntities._

class ValveStateSource(val maxRecordsPerSecond: Int, val probObs: Double)
	extends BaseGenerator[ValveState](maxRecordsPerSecond) {
	val SensorCount: Long = 100000L

	override protected def randomEvent(rnd: SplittableRandom, id: Long): ValveState = {
		val random = rnd.nextDouble()
		val nexState: SensorState = if(random < probObs) SensorObstructed()
		else if((random > probObs) && random < (probObs + ((1-probObs)/2))) SensorOpen()
		else SensorClose()

		 ValveState(sensorId = rnd.nextLong(SensorCount),
			timestamp = System.currentTimeMillis() - rnd.nextLong(1000L),
			value = nexState)
	}
}
