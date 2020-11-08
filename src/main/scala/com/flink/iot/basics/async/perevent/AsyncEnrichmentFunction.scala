package com.flink.iot.basics.async.perevent

import com.flink.iot.basics.db.VehicleOwnerDataClient
import com.flink.iot.basics.entities.VehicleEntities._
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Funcion utilizada para conectarse al CRM y enriquecer el stream
 */
class AsyncEnrichmentFunction extends AsyncFunction[VehicleState, EnrichedVehicleState] {

  /**
   * En la apertura, abrimos las conexiones necesarias
   */
  private val client: VehicleOwnerDataClient = VehicleOwnerDataClient()

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  /**
   * En cada invocacion asincrona, usamos un futuro para retornar el valor enriquecido una vez que el CRM remoto responda
   *
   * @param vehicleState vehicleState
   * @param resultFuture resultFuture
   */
  override def asyncInvoke(vehicleState: VehicleState, resultFuture: ResultFuture[EnrichedVehicleState]): Unit = {
    // issue the asynchronous request, receive a future for the result
    val resultFutureRequested: Future[OwnerReferenceData] = client.asyncGetOwnerReferenceDataFor(vehicleState.vehicleId)

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    resultFutureRequested.onComplete {
      case Success(owner: OwnerReferenceData) => resultFuture.complete(Iterable(EnrichedVehicleState(vehicleState, owner)))
      case Failure(ex) => ex.printStackTrace()
    }
  }
}
