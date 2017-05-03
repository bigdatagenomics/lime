package org.bdgenomics.lime.cli

import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging

private object LimeMain {
  def main(args: Array[String]) = {
    new LimeMain(args).run()
  }
}

private class LimeMain(args: Array[String]) extends Logging {
  private def commands: List[BDGCommandCompanion] = List(Complement,
    Intersection,
    Merge,
    Sort,
    Closest,
    Jaccard,
    Window)

  private def printVersion() {
    println("Version 0")
    // val about = new About()
    // println("\nLime version: " + about.version())
    // if (about.isSnapshot) {
    //   println("Commit: %s Build %s".format(about.commit(), about.buildTimestamp))
    // }
    // println("Built for: Scala %s and Hadoop %s".format(about.scalaVersion(), about.hadoopVersion()))
  }

  private def printCommands() {
    println()
    println("Usage: lime-submit [<spark-args> --] <lime-args> [-version]")
    println()
    println("Choose one of the following commands:")
    println()
    commands.foreach(cmd => {
      println("%20s : %s".format(cmd.commandName, cmd.commandDescription))
    })
    println()
  }

  def run() {
    log.info("Lime invoked with args: " + args.mkString(" "))

    val (versionArgs, nonVersionArgs) = args.partition(_ == "-version")
    if (versionArgs.nonEmpty) {
      printVersion()
    }

    nonVersionArgs.headOption
      .flatMap(cmdName => {
        commands.find(_.commandName == cmdName)
      }).fold({
        printCommands()
      })(cmd => {
        cmd.apply(args.tail).run()
      })
  }
}