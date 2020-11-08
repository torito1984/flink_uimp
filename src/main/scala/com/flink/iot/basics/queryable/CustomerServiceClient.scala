package com.flink.iot.basics.queryable

import com.flink.iot.basics.entities.CustomerEntities._
import com.flink.iot.basics.entities.SensorEntities._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.queryablestate.client.QueryableStateClient

object CustomerServiceClient {

  def main(args: Array[String]) {
    val client = new QueryableStateClient("127.0.0.1", 9069)
    client.setExecutionConfig(new ExecutionConfig())

    val what = args(0)
    val key = args(1).toLong.asInstanceOf[java.lang.Long]
    val jobId = JobID.fromHexString(args(2))

    val complaintDescriptor =
      new ValueStateDescriptor(
        "latestComplaint", // the state name
        classOf[CustomerReport]) // state class

    val alertDescriptor =
      new ValueStateDescriptor(
        "latestAlert", // the state name
        classOf[SensorAlert]) // state class


    if (what.equals("complaint")) {
      val resultFuture = client.getKvState(jobId, "complaints", key, BasicTypeInfo.LONG_TYPE_INFO, complaintDescriptor)
      println(resultFuture)
    } else if (what.equals("alert")) {
      val resultAlert =
        client.getKvState(jobId, "alerts", key, BasicTypeInfo.LONG_TYPE_INFO, alertDescriptor)
      println(resultAlert.get().value())
    }
  }
}
