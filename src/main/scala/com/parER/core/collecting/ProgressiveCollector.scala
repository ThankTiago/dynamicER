package com.parER.core.collecting

import java.io.{BufferedWriter, File, FileWriter}

import com.parER.core.Config
import com.parER.datastructure.Comparison
import org.scify.jedai.datamodel.IdDuplicates
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation
import org.scify.jedai.datamodel.EntityProfile
import java.io.PrintWriter

import scala.collection.mutable.ListBuffer

class ProgressiveCollector(t0: Long, t1: Long, dp: AbstractDuplicatePropagation, printAll: Boolean = true, profiles: Array[(EntityProfile, Int)] = null, attributes: Array[String] = null) {
  import scala.jdk.CollectionConverters._

  var ec = 0.0
  var em = 0.0
  var filling = true
  val duplicates = dp.getDuplicates.asScala
  val buffer = new ListBuffer[String]()
  val nworkers = Config.workers

  def executeAndSave(comparisons: (List[Comparison], Long)) = {
    println(s"Saving comparisons_${comparisons._2}.csv")
    val file = new File(s"comparisons_${comparisons._2}.csv")
    val bw = new PrintWriter(file)
    // add "l_" prefix to attributes
    val l_attributes = attributes.map(a => s"l_${a}")
    val r_attributes = attributes.map(a => s"r_${a}")
    bw.write("l_id, " + l_attributes.mkString(",") + "," + "r_id, " + r_attributes.mkString(",") + "\n")
    for (cmp <- comparisons._1) {
      executeCmp(cmp)
      if (!dp.isSuperfluous(cmp.e1, cmp.e2)) {
        val profile_l = profiles(cmp.e1)._1
        val profile_r = profiles(cmp.e2)._1

        val profile_l_dict = profile_l.getAttributes.asScala.map(a => a.getName -> a.getValue).toMap
        val profile_l_list = attributes.map(a => profile_l_dict.getOrElse(a, ""))
        val profile_r_dict = profile_r.getAttributes.asScala.map(a => a.getName -> a.getValue).toMap
        val profile_r_list = attributes.map(a => profile_r_dict.getOrElse(a, ""))

        bw.write(profile_l.getEntityUrl() + "," + profile_l_list.mkString(",") + "," + profile_r.getEntityUrl() + "," + profile_r_list.mkString(",") + "\n")
      }
    }
    bw.close()
  }

  def comparisonToString(cmp: Comparison) = {
    s"${cmp.e1}\t${cmp.e2}\t${cmp.sim}\n"
  }

  def execute(comparisons: List[Comparison]) = {
    for (cmp <- comparisons) {
      executeCmp(cmp)
    }
  }

  def executeCmp(cmp: Comparison) = {
    //TODO distinguish dirty-cc
    if ( Config.ccer && duplicates.contains(new IdDuplicates(cmp.e1, cmp.e2))) {
      update(cmp.e1, cmp.e2)
    }
    if ( !Config.ccer ) {
      val (a, b) = (duplicates.contains(new IdDuplicates(cmp.e1, cmp.e2)), duplicates.contains(new IdDuplicates(cmp.e2, cmp.e1)))
      if (a || b)
        (a, b) match {
          case (true, _) => update(cmp.e1, cmp.e2)
          case (_, true) => update(cmp.e2, cmp.e1)
        }
    }
    if (printAll)
      print()
    ec += 1
  }

  def update(e1: Int, e2: Int) = {
    if (!dp.isSuperfluous(e1, e2))
      em += 1
  }

  def getPC() = {
    val pc = em / duplicates.size.toDouble
    pc
  }

  def getPQ() = {
    val pq = em / ec
    pq
  }

  def getCardinaliy() = {
    em
  }

  def print() = {
    if (ec % duplicates.size == 0) {
      val ecX = ec / duplicates.size.toDouble
      val rec = em / duplicates.size.toDouble
      val t = System.currentTimeMillis()
      val dt0 = t - t0
      val dt1 = t - t1
      val s = s"${ecX}\t${nworkers}\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}"
      buffer.addOne(s)
      println(s)
    }
  }

  def printLast() = {
    val ecX = ec / duplicates.size.toDouble
    val rec = em / duplicates.size.toDouble
    val pq = em / ec
    val t = System.currentTimeMillis()
    val dt0 = t - t0
    val dt1 = t - t1
    val s = s"${ecX}\t${nworkers}\t${dt0}\t${dt1}\t${rec}\t${em}\t${duplicates.size}\t${if (Config.filling) 1 else 0}\t${ec}\t${pq}"
    buffer.addOne(s)
    println(s)
  }

  def writeFile(filename: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- buffer) {
      bw.write(line)
      bw.newLine()
    }
    bw.close()
  }
}
