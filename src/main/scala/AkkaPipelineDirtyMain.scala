import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, _}
import akka.stream.{FlowShape, OverflowStrategy}
import com.parER.akka.streams.messages._
import com.parER.akka.streams.{ProcessingSeqBatchTokenBlockerStage, ProcessingSeqTokenBlockerStage, StoreSeqBatchModelStage, StoreSeqModelStage}
import com.parER.core.blocking.{BlockGhostingFun, CompGenerationFun}
import com.parER.core.collecting.ProgressiveCollector
import com.parER.core.compcleaning.WNP2CompCleanerFun
import com.parER.core.{Config, TokenizerFun}
import com.parER.datastructure.Comparison
import com.parER.utils.CsvWriter
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation
import java.io.File

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object AkkaPipelineDirtyMain {

  def main(args: Array[String]): Unit = {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Argument parsing
    Config.commandLine(args)
    Config.ccer = false
    val priority = Config.priority
    val dataset1 = Config.dataset1
    val threshold = Config.threshold

    val nBlockers = Config.blockers
    val nWorkers = Config.workers
    val nCleaners = Config.cleaners

    val batchSize = Config.pOption

    // STEP 1. Initialization and read dataset - gt file
    val t0 = System.currentTimeMillis()
    val eFile1  = Config.mainDir + Config.dataset1 + File.separator + Config.dataset1 + "Profiles"
    val gtFile = Config.mainDir + Config.groundtruth + File.separator + Config.groundtruth + "IdDuplicates"

    if (Config.print) {
      println(s"Max memory: ${maxMemory} MB")
      println("File1\t:\t" + eFile1)
      println("gtFile\t:\t" + gtFile)
    }

    val eReader1 = new EntitySerializationReader(eFile1)
    val attributes = scala.io.Source.fromFile(Config.mainDir + Config.dataset1 + File.separator + "schema.txt").mkString.split(",")
    val profiles1 = eReader1.getEntityProfiles().asScala.map((_,0)).toArray
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val gtReader = new GtSerializationReader(gtFile)
    val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

// STEP 2. Initialize stream stages and flow
    implicit val system = ActorSystem("StreamingER")
    implicit val ec = system.dispatcher

    val tokenizer = (lc: Seq[((EntityProfile, Int), Long)]) =>  {
      lc.map( x => x match {case ((e,dId),id) => TokenizerFun.execute(id.toInt, dId, e)} )
    }
    val tokenBlocker = new ProcessingSeqBatchTokenBlockerStage(Config.blocker, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)

    val ff = Config.filteringRatio

    def compGeneration(x: (Message, Long)) = Future.apply {
      x._1 match {
        case BlockTupleSeq(s) => (ComparisonsSeq(s.map(e => Comparisons(CompGenerationFun.generateComparisons(e.id, e.model, e.blocks)))), x._2)
        //case BlockTupleSeq(s) => ComparisonsSeq(s.map(e => Comparisons(CompGenerationFun.generateComparisons(e.id, e.model, BlockGhostingFun.getBlocks(e.blocks, ff)))))
      }
    }

    def compCleaning(s: (ComparisonsSeq, Long)) = Future.apply {
      (ComparisonsSeq(s._1.comparisonSeq.map(e => Comparisons(WNP2CompCleanerFun.execute(e.comparisons)))), s._2)
    }

    val partitionFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val merge = b.add(Merge[(Message, Long)](2))
      val partition = b.add(Partition[(Message, Long)](2, m => {
        m._1 match {
          case UpdateSeq(_) => 0
          case BlockTupleSeq(_) => 1
        }
      }))

      val bGhost = Flow[(Message, Long)].map(x => x._1 match {
        case BlockTupleSeq(s) => ( {
          BlockTupleSeq(s.map(e => {
            val t = BlockGhostingFun.process(e.id, e.model, e.blocks, ff)
            BlockTuple(t._1, t._2, t._3)
          }))
        }, x._2)
      })

      val cGener = Flow[(Message, Long)].mapAsyncUnordered(nBlockers)(compGeneration)
      val compCl = Flow[(ComparisonsSeq, Long)].mapAsyncUnordered(nCleaners)(compCleaning)

      partition.out(0) ~> merge.in(0)
      partition.out(1) ~>
        bGhost.async ~>
        cGener.async ~>
        compCl.buffer(128, OverflowStrategy.backpressure) ~> merge.in(1)
      FlowShape(partition.in, merge.out)
    })

    val program = Source(profiles1).zipWithIndex
      .grouped(batchSize.toInt)
      .zipWithIndex
      .map { case (batch, batchIndex) => (tokenizer(batch), batchIndex) }
      .via(tokenBlocker)
      .via(partitionFlow)
      .via(new StoreSeqBatchModelStage(profiles1.size, 0))

    val t1 = System.currentTimeMillis()
    val proColl = new ProgressiveCollector(t0, t1, dp, false, profiles1, attributes)
    val done = program.runForeach(proColl.executeAndSave)

    done.onComplete( result => {
      println(result)
      val OT = System.currentTimeMillis() - t1
      val ODT = System.currentTimeMillis() - t0
      proColl.printLast()

      if (Config.output) {

        val csv = new CsvWriter("name,OT,ODT,N,Ncg,Ncc,Nw")
        val name = s"ParPipeline+Split"

        // Put dimension of thread pool
        val n = (5+nBlockers+nCleaners+nWorkers).toString
        val ncg = nBlockers.toString
        val ncc = nCleaners.toString
        val nw = nWorkers.toString
        val line = List[String](name, OT.toString, ODT.toString, n, ncg, ncc, nw)
        csv.newLine(line)
        csv.writeFile(Config.file, Config.append)
      }

      system.terminate()
    })
  }
}
