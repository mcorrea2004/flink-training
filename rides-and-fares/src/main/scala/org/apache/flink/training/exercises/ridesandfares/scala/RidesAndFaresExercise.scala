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

package org.apache.flink.training.exercises.ridesandfares.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.{RideAndFare, TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.training.exercises.common.utils.MissingSolutionException
import org.apache.flink.util.Collector

/** The Stateful Enrichment exercise from the Flink training.
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  */
object RidesAndFaresExercise {

  class RidesAndFaresJob(
      rideSource: SourceFunction[TaxiRide],
      fareSource: SourceFunction[TaxiFare],
      sink: SinkFunction[RideAndFare]
  ) {

    def execute(): JobExecutionResult = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val rides = env
        .addSource(rideSource)
        .filter { ride =>
          ride.isStart
        }
        .keyBy { ride =>
          ride.rideId
        }

      val fares = env
        .addSource(fareSource)
        .keyBy { fare =>
          fare.rideId
        }

      rides
        .connect(fares)
        .flatMap(new EnrichmentFunction())
        .addSink(sink)

      env.execute()
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job =
      new RidesAndFaresJob(new TaxiRideGenerator, new TaxiFareGenerator, new PrintSinkFunction)

    job.execute()
  }
/*
Program Structure
You can use a RichCoFlatMap to implement this join operation. Note that you have no control over the order of arrival of the ride and fare records for each rideId,
so you'll need to be prepared to store either piece of information until the matching info arrives, at which point you can emit a RideAndFare joining the two records together.

Working with State
You should be using Flink's managed, keyed state to buffer the data that is being held until the matching event arrives. And be sure to clear the state once it is no longer needed.


For the purposes of this exercise it's okay to assume that the START and fare events are perfectly paired. But in a real-world application you should worry about the fact that whenever an event is missing,
the other event for the same rideId will be held in state forever. In a later lab we'll look at the ProcessFunction and Timers which may also help the situation here.
 */
  class EnrichmentFunction() extends RichCoFlatMapFunction[TaxiRide, TaxiFare, RideAndFare] {

    private var rideState: ValueState[TaxiRide] = _
    private var fareState: ValueState[TaxiFare] = _

    override def open(parameters: Configuration): Unit = {
//      throw new MissingSolutionException()

      /*
      Creates two stateful properties (rideState, fareState) to be fulfilled with each expected object (TaxiRide, TaxiFare) when it is received in any of the flatMap methods.
      State is kept by Flink system (it comes from Flink.context.FlinkState), regardless the instance of the class,
      until we clear it once we've achieved the expected output, in this case it is when we have the two objects (TaxiRide, TaxiFare)
       */

      rideState = getRuntimeContext.getState(
        // This creates a stateful descriptor to hold a TaxiRide object as a value
        new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide])
      )

      fareState = getRuntimeContext.getState(
        // This creates a stateful descriptor to hold a TaxiFare object as a value
        new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare])
      )
    }

    override def flatMap1(ride: TaxiRide, out: Collector[RideAndFare]): Unit = {
      //      throw new MissingSolutionException()


      // At this stage, inputs have already been grouped by rideID as specified in each data stream source


      // Get the value / object from the stateful descriptor / property
      val fare = fareState.value

      /*
      If not null, it means that the state has already received a fare object in a previous call / instance, so we can complete the map by creating a RideAndFare object
      and adding it to the collection give as an argument.
      State must be clear at this point as well, so the process can restart to match another pair of ride and fare for a different rideID
       */
      if (fare != null) {
        fareState.clear()
        out.collect(new RideAndFare(ride, fare))
      } else {
        // If null, it means that the state has not received a fare object yet, so we update the ride state with the given TaxiRade object
        rideState.update(ride)
      }
    }

    override def flatMap2(fare: TaxiFare, out: Collector[RideAndFare]): Unit = {
//      throw new MissingSolutionException()
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        out.collect(new RideAndFare(ride, fare))
      } else {
        fareState.update(fare)
      }
    }
  }

}
