package com.parER.akka.streams

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import com.parER.akka.streams.messages.{Comparisons, ComparisonsSeq, Message, UpdateSeq}
import com.parER.core.blocking.StoreModel
import com.parER.datastructure.Comparison

class StoreSeqBatchModelStage(val size1: Int = 16, val size2: Int = 16) extends GraphStage[FlowShape[(Message, Long), (List[Comparison], Long)]] {
  val in = Inlet[(Message, Long)]("StoreSeqModelStage.in")
  val out = Outlet[(List[Comparison], Long)]("StoreSeqModelStage.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      val storeModel = new StoreModel(size1, size2)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val items   = grab(in)

          items._1 match {
            case UpdateSeq(updates) => {
              updates.map( u => storeModel.solveUpdate(u.id, u.model) )
              pull(in)
            }

            case Comparisons(lc) =>
              val llc = storeModel.solveComparisons(lc)
              if (llc.size > 0) {
                push(out, (llc, items._2))
              } else {
                pull(in)
              }

            case ComparisonsSeq(s) => {
              val llc = List.newBuilder[Comparison]
              for (lc <- s) {
                val comps = storeModel.solveComparisons(lc.comparisons)
                llc ++= comps
              }
              if (llc.knownSize > 0) {
                push(out, (llc.result(), items._2))
              } else {
                pull(in)
              }
            }
          }
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }
      })
    }

}