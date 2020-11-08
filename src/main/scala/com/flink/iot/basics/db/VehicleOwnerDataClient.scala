package com.flink.iot.basics.db

import com.flink.iot.basics.entities.VehicleEntities._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object VehicleOwnerDataClient {
  val MaxParallelism: Int = 30
  private val Rand = new Random(42)

  private val fName = List("Romeo", "Blake", "Aarav", "Cillian", "Lorenzo", "Aleksander", "Vincent", "Ted", "Elliot",
    "Ralphy", "Melissa", "Dolly", "Fern", "Hollie", "Evie", "Nancy", "Daniella", "Olivia-Rose", "Delilah", "Norah",
    "Jo", "Skylar", "Ali", "Kit", "Jackie", "Glenn", "Kai", "Alex", "Charlie", "Reggie")
  private val lName = List("Harvey", "Baker", "Saunders", "Green", "Houghton", "Kennedy", "Russell", "Wood", "Cole",
    "Kennedy", "Berry", "Day", "Mitchell", "Jackson", "Chambers", "Robertson", "Matthews", "Bailey", "Reynolds",
    "Lloyd", "Johnston", "Lloyd", "Bates", "Ward", "Hunter", "Harper", "Austin", "Kennedyv", "Fletcher", "Bates")

  def getRandomReferenceDataFor(vehicleId: Long): OwnerReferenceData = {
    OwnerReferenceData(s"${fName((vehicleId % fName.size).toInt)} ${lName((vehicleId % lName.size).toInt)}",
      Rand.nextDouble() > 0.95)
  }
}

case class VehicleOwnerDataClient() {

  import VehicleOwnerDataClient._

  def asyncGetOwnerReferenceDataFor(vehicleId: Long)(implicit ec: ExecutionContext): Future[OwnerReferenceData] = {
    Future {
      Thread.sleep(Rand.nextInt(5) + 5)
      getRandomReferenceDataFor(vehicleId)
    }
  }
}
