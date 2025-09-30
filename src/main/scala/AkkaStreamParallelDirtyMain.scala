import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{FlowShape, OverflowStrategy}
import com.parER.akka.streams.messages.{Comparisons, Message, Update}
import com.parER.akka.streams.{ProgressiveCollectorSink, StoreModelStage, TokenBlockerStage}
import com.parER.core.compcleaning.ComparisonCleaning
import com.parER.core.{Config, Tokenizer}
import com.parER.datastructure.Comparison
import org.scify.jedai.datamodel.EntityProfile
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.textmodels.TokenNGrams
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation

import scala.concurrent.Future

import java.io.File

object AkkaStreamParallelDirtyMain {

  def main(args: Array[String]): Unit = {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Argument parsing
    Config.commandLine(args)
    val priority = Config.priority
    val dataset1 = Config.dataset1
    val threshold = Config.threshold

    val nBlockers = Config.blockers
    val nWorkers = Config.workers
    val nCleaners = Config.cleaners

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
    val profiles1 = eReader1.getEntityProfiles.asScala.map((_,0)).toArray
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val gtReader = new GtSerializationReader(gtFile)
    val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    // STEP 2. Initialize stream stages and flow
    implicit val system = ActorSystem("QuickStart") // TODO optional?
    implicit val ec = system.dispatcher

    val tokenizer: (((EntityProfile, Int), Long)) => Future[(Int, TokenNGrams)] = {
      case ((e,dId),id) => {
        println("dId: " + dId + " id: " + id)
        Future.apply{ new Tokenizer().execute(id.toInt, dId, e) }
      }
    }

    val tokenBlocker = new TokenBlockerStage(Config.blocker, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)
    val compCleaner = (lc: List[Comparison]) => Future.apply{ ComparisonCleaning.apply(Config.ccMethod).execute(lc) }
    val matcherFun = (c: Comparison) => {c.sim = c.e1Model.getSimilarity(c.e2Model); c}
    val matcher = (lc: List[Comparison]) => Future.apply{ lc.map(matcherFun) }
    val storeModel = new StoreModelStage

    val t1 = System.currentTimeMillis()
    val collector = new ProgressiveCollectorSink(t0, t1, dp, Config.print)

    val partitionFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val partition = b.add(Partition[Message](2, m => {
        m match {
          case Update(_,_) => 0
          case Comparisons(_) => 1
        }
      }))
      val merge = b.add(MergePreferred[Message](1))
      val convert1 = b.add(Flow[Message].map (x => x match { case Comparisons(comparisons) => comparisons }))
      val ccStage = b.add(Flow[List[Comparison]].mapAsyncUnordered(nCleaners)(compCleaner).async)
      val convert2 = b.add(Flow[List[Comparison]].map (x => new Comparisons(x)))
      partition.out(0) ~> merge.in(0)
      partition.out(1) ~> convert1 ~> ccStage ~> convert2 ~> merge.in(1)
      FlowShape(partition.in, merge.out)
    })

    val incrementalFlow = Flow[(Int, TokenNGrams)]
      .via(tokenBlocker)
      .buffer(16, OverflowStrategy.backpressure)
      .via(partitionFlow)
      .via(storeModel)

    val program =
      Source(profiles1).zipWithIndex
      .mapAsync(1)(tokenizer)
      .via(tokenBlocker)
      .buffer(16, OverflowStrategy.backpressure)
      .via(partitionFlow)
      .via(storeModel)

    /*val program2 =
      Source(profiles1).zipWithIndex
      .grouped(200)
      .mapAsync(1) { batch =>
        println("batch: " + batch.size)
        val tokenizedBatch = batch.map(tokenizer)
          
        tokenizedBatch.flatMap { tokenizedProfiles =>
          println("tokenizedProfiles: " + tokenizedProfiles.size)
          tokenizedProfiles.via(incrementalFlow)
        }
      }
      .mapConcat(identity) // Flatten the results from each batch
    */
    val done = program.runWith(collector)

    done.onComplete( result => {
      println("done")
      val t4 = System.currentTimeMillis()
      val counter = result.get
      system.terminate()
    })
  }
}


