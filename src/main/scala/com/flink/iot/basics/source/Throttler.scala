package com.flink.iot.basics.source

class Throttler(maxRecordsPerSecond: Long, numberOfParallelSubtasks: Int) {
  require(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
    "maxRecordsPerSecond must be positive or -1 (infinite)")
  require(numberOfParallelSubtasks > 0, "numberOfParallelSubtasks must be greater than 0")

  // unlimited speed
  private var throttleBatchSize: Long = -1
  private var nanosPerBatch: Long = 0
  private var endOfNextBatchNanos: Long = System.nanoTime() + nanosPerBatch
  private var currentBatch: Int = 0

  if (maxRecordsPerSecond != -1) {
    val ratePerSubtask: Float = maxRecordsPerSecond.toFloat / numberOfParallelSubtasks

    if (ratePerSubtask >= 10000) {
      // high rates: all throttling in intervals of 2ms
      throttleBatchSize = (ratePerSubtask / 500).toInt
      nanosPerBatch = 2000000L
    } else {
      throttleBatchSize = (ratePerSubtask / 20).toInt + 1
      nanosPerBatch = (1000000000L / ratePerSubtask).toInt * throttleBatchSize
    }
    this.endOfNextBatchNanos = System.nanoTime() + nanosPerBatch
  }

  def throttle(): Unit = {
    currentBatch += 1
    if ((throttleBatchSize != -1) && (currentBatch == throttleBatchSize)) {
      currentBatch = 0

      val now = System.nanoTime()
      val millisRemaining = ((endOfNextBatchNanos - now) / 1000000).toInt

      if (millisRemaining > 0) {
        endOfNextBatchNanos += nanosPerBatch
        Thread.sleep(millisRemaining)
      } else {
        endOfNextBatchNanos = now + nanosPerBatch
      }
    }
  }
}
