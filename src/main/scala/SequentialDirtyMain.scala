
import SequentialCCMain.{blockGhoster, cBlocker, compGeneration, dataset1, proCollector, storeModel, tBlockGhosting, tBlocker, tCollector, tCompCleaner, tCompGeneration, tMatcher, tStoreModel, tTokenizer, tokenBlocker}
import com.parER.core.blocking.{BlockGhosting, Blocking, CompGeneration, StoreModel}
import com.parER.core.collecting.ProgressiveCollector
import com.parER.core.compcleaning.ComparisonCleaning
import com.parER.core.matching.JSMatcher
import com.parER.core.{Config, Tokenizer}
import com.parER.datastructure.Comparison
import com.parER.utils.CsvWriter
import org.scify.jedai.datareader.entityreader.EntitySerializationReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation
import java.io.File

object SequentialDirtyMain extends App {
    import scala.jdk.CollectionConverters._

    val maxMemory = Runtime.getRuntime().maxMemory() / math.pow(10, 6)

    // Time variables
    var tTokenizer = 0L
    var tBlocker = 0L
    var tCompCleaner = 0L
    var tMatcher = 0L
    var tCollector = 0L
    var tStoreModel = 0L
    var tBlockGhosting = 0L
    var tCompGeneration = 0L

    // Comparison counters
    var cBlocker = 0L
    var cCompCleaner = 0L

    // Argument parsing
    Config.commandLine(args)
    val priority = Config.priority
    val dataset1 = Config.dataset1
    val dataset2 = Config.dataset2
    val threshold = Config.threshold
    val smBool = Config.storeModel

    println(s"Store model ${smBool}")

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
    val profiles1 = eReader1.getEntityProfiles.asScala.toArray
    if (Config.print) System.out.println("Input Entity Profiles1\t:\t" + profiles1.size)

    val gtReader = new GtSerializationReader(gtFile)
    val dp = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(null))
    if (Config.print) System.out.println("Existing Duplicates\t:\t" + dp.getDuplicates.size)

    // STEP 2. functional stages
    val tokenizer = new Tokenizer
    val tokenBlocker = Blocking.apply(Config.blocker, profiles1.size, 0, Config.cuttingRatio, Config.filteringRatio)
    val compCleaner = ComparisonCleaning.apply(Config.ccMethod)
    val compMatcher = new JSMatcher
    val proCollector = new ProgressiveCollector(t0, System.currentTimeMillis(), dp, Config.print)
    val blockGhoster = new BlockGhosting(Config.filteringRatio)
    val compGeneration = new CompGeneration

    val storeModel = new StoreModel
    tokenBlocker.setModelStoring(!smBool)

    val t1 = System.currentTimeMillis()
    // STEP 3. iterative computation -> (Assume CCER)
    var i = 0
    val n = profiles1.length
    while (i < n) {
        var t = 0L
        var comps1 = List[Comparison]()

        t = System.currentTimeMillis()
        val (id1, obj1) = tokenizer.execute(i, 0, profiles1(i))
        tTokenizer += (System.currentTimeMillis() - t)

        t = System.currentTimeMillis()
        val tuple = tokenBlocker.process(id1, obj1)
        tBlocker += (System.currentTimeMillis() - t)
        //cBlocker += comps1.size

        t = System.currentTimeMillis()
        val bIds = blockGhoster.process(tuple._1, tuple._2, tuple._3)
        tBlockGhosting += (System.currentTimeMillis() - t)

        t = System.currentTimeMillis()
        comps1 = compGeneration.generateComparisons(bIds._1, bIds._2, bIds._3)
        tCompGeneration += (System.currentTimeMillis() - t)
        cBlocker += comps1.size

        t = System.currentTimeMillis()
        comps1 = compCleaner.execute(comps1)
        tCompCleaner += (System.currentTimeMillis() - t)
        cCompCleaner += comps1.size

        t = System.currentTimeMillis()
        storeModel.solveUpdate(id1, obj1)
        comps1 = storeModel.solveComparisons(comps1)
        tStoreModel += (System.currentTimeMillis() - t)

        t = System.currentTimeMillis()
        comps1 = compMatcher.execute(comps1)
        tMatcher += (System.currentTimeMillis() - t)

        t = System.currentTimeMillis()
        proCollector.execute(comps1)
        tCollector += (System.currentTimeMillis() - t)

        i += 1
    }

    proCollector.printLast()

    println("\nTime measurements: ")
    println("tTokenizer = " + tTokenizer + " ms")
    println("tBlocker = " + tBlocker + " ms")
    println("tCompCleaner = " + tCompCleaner + " ms")
    println("tMatcher = " + tMatcher + " ms")
    println("tCollector = " + tCollector + " ms")
    println("tStoreModel = " + tStoreModel + " ms")

    println("PC = " + proCollector.getPC)
    println("Comparisons after blocking: " + cBlocker)
    println("Comparisons after comparison cleaning: " + cCompCleaner)

    if (Config.output) {
        val csv = new CsvWriter("name,dr,bb+bp,bg,cg,cc,lm,co,cl,sum,RT,PC")
        val name = dataset1+Config.cuttingRatio
        val dr = tTokenizer
        val bb_bp = tBlocker
        val bg = tBlockGhosting
        val cg = tCompGeneration
        val cc = tCompCleaner
        val lm = tStoreModel
        val co = tMatcher
        val cl = tCollector
        val sum = dr+bb_bp+bg+cg+cc+lm+co+cl
        val RT = (System.currentTimeMillis() - t1).toString
        val PC = proCollector.getPC.toString

        val line = List[String](name,dr.toString,bb_bp.toString,bg.toString,cg.toString,cc.toString,lm.toString,co.toString,cl.toString,sum.toString,RT.toString,PC)
        println(name)
        println(line)
        csv.newLine(line)
        csv.writeFile(Config.file, Config.append)
    }

    //    if (Config.output) {
//        val csv = new CsvWriter("name, CoCl, ro, ff, PC, PQ, BB(T), CC(T), MA(T), O(T), OD(T), BB(C), CC(C)")
//
//        val CoCl = Config.ccMethod
//        val ro = Config.cuttingRatio.toString
//        val ff = Config.filteringRatio.toString
//        val p = if (smBool) "sm-" else ""
//        val name = p+CoCl + "-" + ro + "-" + ff
//
//        val PC = proCollector.getPC.toString
//        val PQ = proCollector.getPQ.toString
//        val BBT = (tTokenizer + tBlocker + tStoreModel).toString
//        val CCT = tCompCleaner.toString
//        val MAT = tMatcher.toString
//        val OT = (tTokenizer+tBlocker+tCompCleaner+tMatcher).toString
//        val ODT = (System.currentTimeMillis() - t0).toString
//        val BBC = cBlocker.toString
//        val CCC = cCompCleaner.toString
//
//        val line = List[String](name, CoCl, ro, ff, PC, PQ, BBT, CCT, MAT, OT, ODT, BBC, CCC)
//        csv.newLine(line)
//        csv.writeFile(Config.file, Config.append)
//    }
}