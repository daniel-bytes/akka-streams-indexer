package com.example

import akka.stream.Attributes._
import akka.stream._
import akka.stream.stage._

import scala.concurrent.duration._
import scala.util.Random

/**
  * Toggle state for creating and releasing back pressure in a stream
  * @param initialState The initial state of the toggle (true to allow messages to flow)
  * @param key The name of the toggle (defaults to random string)
  * @param timerGranularity The granularity of the internal timer used to check the state of the toggle
  */
class ToggleState(
  initialState: Boolean,
  val key: String = Random.nextString(10),
  val timerGranularity: FiniteDuration = 100 milliseconds
) extends Attribute
{
  private var _state: Boolean = initialState

  def state: Boolean = _state
  def on(): Boolean = { _state = true; _state }
  def off(): Boolean = { _state = false; _state }
  def toggle(): Boolean = { _state = !_state; _state }
}

/**
  * Helper for creating ToggleState stream attributes
  */
object ToggleAttributes
{
  def toggleState(state: ToggleState): Attributes = Attributes(state :: Nil)
}

/**
  * Flow stage for toggling backpressure via a [[ToggleState]] attribute.
  *
  * Example:
  *   import ToggleAttributes
  *   val toggle = new ToggleState(true)
  *
  *   Flow[T]
  *     .via(
  *       new Toggle[Message]().withAttributes(ToggleAttributes.toggleState(toggle))
  *     )
  *     // etc
  *
  *   toggle.off() // block stream, causing backpressure
  *   toggle.on()  // unblock stream
  */
class Toggle[T] extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet("Toggle.in")
  val out: Outlet[T] = Outlet("Toggle.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      // Pull in the ToggleState from supplied attributes,
      // or else default to an open toggle
      var toggle = inheritedAttributes
        .attributeList
        .collectFirst { case x: ToggleState => x }
        .getOrElse(new ToggleState(true))

      override def onPush(): Unit = {
        push(out, grab(in))
      }

      override def onPull(): Unit = {
        if (toggle.state) {
          pull(in)
        } else {
          scheduleOnce(toggle.key, toggle.timerGranularity)
        }
      }

      override def onTimer(timerKey: Any): Unit = {
        if (toggle.state) {
          pull(in)
        } else {
          scheduleOnce(toggle.key, toggle.timerGranularity)
        }
      }

      setHandlers(in, out, this)
    }
}