package com.flink.examples

import org.apache.flink.streaming.api.scala._

/**
 * Skeleton for a Flink Job.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     * env.readTextFile(textPath);
     *
     * then, transform the resulting DataSet[String] using operations
     * like:
     * .filter()
     * .flatMap()
     * .join()
     * .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/programming_guide.html
     *
     * and the examples
     *
     * http://flink.apache.org/docs/latest/examples.html
     *
     */

    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
