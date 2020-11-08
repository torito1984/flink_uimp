package com.flink.iot.basics.source

import java.util.SplittableRandom

import com.flink.iot.basics.entities.CustomerEntities._

class ClientCommunicationSource(val maxRecordsPerSecond: Int, val probObs: Double)
  extends BaseGenerator[CustomerReport](maxRecordsPerSecond) {
  val SensorCount: Long = 100000L

  private val fName = List("Romeo", "Blake", "Aarav", "Cillian", "Lorenzo", "Aleksander", "Vincent", "Ted", "Elliot",
    "Ralphy", "Melissa", "Dolly", "Fern", "Hollie", "Evie", "Nancy", "Daniella", "Olivia-Rose", "Delilah", "Norah",
    "Jo", "Skylar", "Ali", "Kit", "Jackie", "Glenn", "Kai", "Alex", "Charlie", "Reggie")
  private val lName = List("Harvey", "Baker", "Saunders", "Green", "Houghton", "Kennedy", "Russell", "Wood", "Cole",
    "Kennedy", "Berry", "Day", "Mitchell", "Jackson", "Chambers", "Robertson", "Matthews", "Bailey", "Reynolds",
    "Lloyd", "Johnston", "Lloyd", "Bates", "Ward", "Hunter", "Harper", "Austin", "Kennedyv", "Fletcher", "Bates")

  override protected def randomEvent(rnd: SplittableRandom, id: Long): CustomerReport = {

    val random = rnd.nextDouble()
    val nexState = if (random < probObs) "BROKEN_DOWN"
    else if ((random > probObs) && random < (probObs + ((1 - probObs) / 4))) "DISSATISFIED"
    else "SATISFIED"

    val vehicleId = rnd.nextLong(SensorCount) / 100
    CustomerReport(vehicleId = vehicleId,
      timestamp = System.currentTimeMillis() - rnd.nextLong(1000L),
      value = nexState,
      customerName = s"${fName((vehicleId % fName.size).toInt)} ${lName((vehicleId % lName.size).toInt)}")
  }
}
