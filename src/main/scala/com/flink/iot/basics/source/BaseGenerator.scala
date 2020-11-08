package com.flink.iot.basics.source

import java.util.SplittableRandom
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

abstract class BaseGenerator[T](maxRecordsPerSecond: Int) extends RichParallelSourceFunction[T]
  with CheckpointedFunction {
  require(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0, "maxRecordsPerSecond must be positive or -1 (infinite)")

  private var running = true
  private var id: Long = _
  private var idState: ListState[Long] = _

  override def open(parameters: Configuration): Unit = {
    if (id == -1) id = getRuntimeContext.getIndexOfThisSubtask
  }

  protected def randomEvent(rnd: SplittableRandom, id: Long): T

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    val numberOfParallelSubtasks = getRuntimeContext.getNumberOfParallelSubtasks
    val throttler = new Throttler(maxRecordsPerSecond, numberOfParallelSubtasks)
    val rnd = new SplittableRandom()

    val lock = ctx.getCheckpointLock

    while (running) {
      val event = randomEvent(rnd, id)

      lock.synchronized {
        ctx.collect(event)
        id += numberOfParallelSubtasks
      }

      throttler.throttle()
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    idState.clear()
    idState.add(id)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    idState =
      context
        .getOperatorStateStore
        .getUnionListState(new ListStateDescriptor("ids", createTypeInformation[Long]))

    if (context.isRestored) {
      val max = idState.get().asScala.max
      id = max + getRuntimeContext.getIndexOfThisSubtask.toLong
    }
  }
}
