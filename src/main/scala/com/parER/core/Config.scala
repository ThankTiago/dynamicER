package com.parER.core

import java.io.File

object Config {

  val mainDir = System.getProperty("user.dir") + File.separator + ".." + File.separator + ".." + File.separator + ".." + File.separator + ".." + File.separator + "data" + File.separator + "blocking" + File.separator + "task" + File.separator

  var dataset1 = ""
  var dataset2 = ""
  var groundtruth = ""
  var pardegree = 1
  var cleaners = 1
  var workers = 1
  var ccer = false
  var priority = false
  var threshold = 0.5
  var priorityThreshold = 0.0
  var filteringRatio = 0.01
  var cuttingRatio = 0.005
  var prioritizer = ""
  var pOption = 500L
  var pOption2 = 50L
  var print = true
  var blocker = "tbr"
  var ccMethod = "wnp2"
  var matcher = "js"
  var file = ""
  var output = false
  var append = true
  var blockers = 1
  var storeModel = true
  var batches = 1

  var filling = false

  def getsubDir() = {
    if (!ccer) "dirtyErDatasets" + File.separator
    else "cleanCleanErDatasets" + File.separator
  }

  // argument parsing
  def commandLine(args: Array[String]) = {
    args.sliding(2,2).toList.collect{
      // long options
      case Array("--nworkers", arg: String) => Config.workers = arg.toInt
      case Array("--ncleaners", arg: String) => Config.cleaners = arg.toInt
      case Array("--nblockers", arg: String) => Config.blockers = arg.toInt
      case Array("--dataset1", arg: String) => Config.dataset1 = arg
      case Array("--dataset2", arg: String) => Config.dataset2 = arg
      case Array("--groundtruth", arg: String) => Config.groundtruth = arg
      case Array("--priority", arg: String) => { Config.priority = true ; Config.prioritizer = arg }
      case Array("--blocker", arg: String) => Config.blocker = arg
      case Array("--threshold", arg: String) => Config.threshold = arg.toDouble
      case Array("--pthreshold", arg: String) => Config.priorityThreshold = arg.toDouble
      case Array("--poption", arg: String) => Config.pOption = arg.toLong
      case Array("--poption2", arg: String) => Config.pOption2 = arg.toLong
      case Array("--filtering", arg: String) => Config.filteringRatio = arg.toDouble
      case Array("--cutting", arg: String) => Config.cuttingRatio = arg.toDouble
      case Array("--print", arg: String) => Config.print = arg.toInt != 0
      case Array("--comparisoncleaning", arg: String) => Config.ccMethod = arg
      case Array("--matcher", arg: String) => Config.matcher = arg
      case Array("--output", arg: String) => { Config.output = true ; Config.file = arg}
      case Array("--append", arg: String) => Config.append = arg.toInt != 0
      case Array("--storemodel", arg: String) => Config.storeModel = arg.toInt != 0
      case Array("--batches", arg: String) => Config.batches = arg.toInt

      // short options
      case Array("-nw", arg: String) => Config.workers = arg.toInt
      case Array("-nc", arg: String) => Config.cleaners = arg.toInt
      case Array("-nb", arg: String) => Config.blockers = arg.toInt
      case Array("-d1", arg: String) => Config.dataset1 = arg
      case Array("-d2", arg: String) => {Config.ccer = true; Config.dataset2 = arg}
      case Array("-gt", arg: String) => Config.groundtruth = arg
      case Array("-p", arg: String) => { Config.priority = true ; Config.prioritizer = arg}
      case Array("-b", arg: String) => Config.blocker = arg
      case Array("-t", arg: String) => Config.threshold = arg.toDouble
      case Array("-pt", arg: String) => Config.priorityThreshold = arg.toDouble
      case Array("-popt", arg: String) => Config.pOption = arg.toLong
      case Array("-popt2", arg: String) => Config.pOption2 = arg.toLong
      case Array("-fi", arg: String) => Config.filteringRatio = arg.toDouble
      case Array("-cu", arg: String) => Config.cuttingRatio = arg.toDouble
      case Array("-bc", arg: String) => Config.cuttingRatio = arg.toDouble
      case Array("-print", arg: String) => Config.print = arg.toInt != 0
      case Array("-cc", arg: String) => Config.ccMethod = arg
      case Array("-m", arg: String) => Config.matcher = arg
      case Array("-o", arg: String) => { Config.output = true ; Config.file = arg}
      case Array("-a", arg: String) => Config.append = arg.toInt != 0
      case Array("-sm", arg: String) => Config.storeModel = arg.toInt != 0
    }
  }

  def setCcer(v: Boolean) = {
    ccer = v
  }
}
