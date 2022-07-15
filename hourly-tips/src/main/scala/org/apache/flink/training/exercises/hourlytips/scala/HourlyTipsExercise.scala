/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.training.exercises.common.utils.MissingSolutionException
import org.apache.flink.util.Collector

/** The Hourly Tips exercise from the Flink training.
  *
  * The task of the exercise is to first calculate the total tips collected by each driver,
  * hour by hour, and then from that stream, find the highest tip total in each hour.
  */
object HourlyTipsExercise {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job = new HourlyTipsJob(new TaxiFareGenerator, new PrintSinkFunction)

    job.execute()
  }

  class HourlyTipsJob(source: SourceFunction[TaxiFare], sink: SinkFunction[(Long, Long, Float)]) {

    /** Create and execute the ride cleansing pipeline.
      */
    @throws[Exception]
    def execute(): JobExecutionResult = {
      // replace this with your solution
      /*if (true) {
        throw new MissingSolutionException
      }*/

      // start the data generator
      /*val fares: DataStream[TaxiFare] = env.addSource(source)
      fares
        .keyBy(_.driverId) // Group and split the data stream by driverID
        .window(TumblingEventTimeWindows.of(Time.hours(1))) // Define the window assignment, in this case a simple window of 1 hour
        .process(new MaximumTip())
        .addSink(sink)*/

      // Get the Flink execution environment
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // Set up teh watermark strategy, for the ordered (by timestamp) taxi fare stream
      val watermarkStrategy = WatermarkStrategy
        .forMonotonousTimestamps[TaxiFare]()
        .withTimestampAssigner(new SerializableTimestampAssigner[TaxiFare] {
          override def extractTimestamp(fare: TaxiFare, streamRecordTimestamp: Long): Long =
            fare.getEventTimeMillis
        })

      // Set up the pipeline
      env
        .addSource(source) // Add the source data stream to Flink execution environment
        .assignTimestampsAndWatermarks(watermarkStrategy) // Assign the watermarks strategy
        .map((f: TaxiFare) => (f.driverId, f.tip)) // Map each TaxiFare model to a (Long, Float) tuple containing only the needed information
        .keyBy(_._1) // // Group and split the data stream by driverID
        .window(TumblingEventTimeWindows.of(Time.hours(1))) // Define the window assignment, in this case a simple window of 1 hour
        .reduce(
          (f1: (Long, Float), f2: (Long, Float)) => { (f1._1, f1._2 + f2._2) },
          new MaximumTip()
        ) // This reduces all tips to a single one per driverID, containing the driverID and the sum of the tips that belong to the same driverID
        .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
        .maxBy(2) // It gets the maximum of all items coming out from the data stream
        .addSink(sink) // It puts the result in the given sink

      // execute the pipeline and return the result
      env.execute("Hourly Tips")
    }
  }
}

class MaximumTip extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
  override def process(
                        key: Long,
                        context: Context,
                        elements: Iterable[(Long, Float)],
                        out: Collector[(Long, Long, Float)]
                      ): Unit = {
/*    val max = elements.reduce((fare1, fare2) => {
      Math.max(fare1.tip, fare2.tip)
    })*/

    val sumOfTips = elements.iterator.next()._2
    out.collect((context.window.getEnd, key, sumOfTips))
  }
}
