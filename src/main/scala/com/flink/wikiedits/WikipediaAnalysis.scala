package com.flink.wikiedits

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource

/**
 * Ejercicio 21: Acceso al stream de cambios de wikipedia
 *
 * Este codigo de ejemplo calcula los cambios hecho a wikipedia, agrupados por usuario, er los ultimos 5 segundos.
 * Tomar como punto de partida para un ejercicio libre.
 *
 */
object WikipediaAnalysis {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val edits = env.addSource(new WikipediaEditsSource())

    val keyedEdits = edits.keyBy(_.getUser)

    val result = keyedEdits
      .timeWindow(Time.seconds(5))
      .aggregate(new AggregateFunction[WikipediaEditEvent, (String, Long), (String, Long)] {
        override def createAccumulator(): (String, Long) = ("", 0L)

        override def add(value: WikipediaEditEvent, accumulator: (String, Long)): (String, Long) = {
          (value.getUser, value.getByteDiff + accumulator._2)
        }

        override def getResult(accumulator: (String, Long)): (String, Long) = accumulator

        override def merge(a: (String, Long), b: (String, Long)): (String, Long) = (a._1, a._2 + b._2)
      })

    result.print()

    env.execute()
  }
}
