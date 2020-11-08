package com.flink.iot.basics.operators

import com.flink.iot.basics.entities.SensorEntities._
import org.apache.flink.api.common.functions.AggregateFunction

class DoubleSensorAverageAggregate
  extends AggregateFunction[SensorValue, (Double, Double, Double, Double), (Double, Double, Double, Double)] {

  private val Primary = "PRIMARY"

  override def createAccumulator(): (Double, Double, Double, Double) = {
    (0.0, 0.0, 0.0, 0.0)
  }

  override def add(value: SensorValue,
                   accumulator: (Double, Double, Double, Double)): (Double, Double, Double, Double) = {
    value match {
      case SensorValue(Primary, _, _, _, value) =>
        (accumulator._1 + 1, accumulator._2 + value, accumulator._3, accumulator._4)
      case SensorValue(_, _, _, _, value) =>
        (accumulator._1 + 1, accumulator._2 + value, accumulator._3 + 1, accumulator._4 + value)
    }
  }

  override def getResult(accumulator: (Double, Double, Double, Double)): (Double, Double, Double, Double) = {
    accumulator
  }

  override def merge(a: (Double, Double, Double, Double),
                     b: (Double, Double, Double, Double)): (Double, Double, Double, Double) = {
    (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
  }
}