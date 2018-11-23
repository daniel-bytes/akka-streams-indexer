package com.example

import akka.stream._
import akka.stream.stage._

class ToggleState
(
  initialState: Boolean
)
{
  private var _state: Boolean = initialState

  def state: Boolean = _state
  def on(): Boolean = { _state = true; _state }
  def off(): Boolean = { _state = false; _state }
  def toggle(): Boolean = { _state = !_state; _state }
}

class ToggleBackpressure[T](toggle: ToggleState) extends GraphStage[FlowShape[T, T]] {
  val in: Inlet[T] = Inlet("ToggleShape.in")
  val out: Outlet[T] = Outlet("ToggleShape.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    override def onPush(): Unit = {
      push(out, grab(in))
    }

    override def onPull(): Unit = {
      if (toggle.state) {
        pull(in)
      }
    }

    setHandlers(in, out, this)
  }
}