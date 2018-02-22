/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
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
    Cluster,
    Intersection,
    Jaccard,
    Merge,
    Sort,
    Closest,
    Window)

  private def printLogo() {
    print("\n")
    print("""          _ _
             |  .;++   | (_)
             |  ;++#   | |_ ____   ____
             |  ;++#   | | |    \ / _  )
             |  +++#   | | | | | ( (/ /
             |  +##.   |_|_|_|_|_|\____)""".stripMargin('|'))
  }

  private def printVersion() {
    printLogo()
    val about = new About()
    println("\n\nLime version: " + about.version())
    if (about.isSnapshot) {
      println("Commit: %s Build %s".format(about.commit(), about.buildTimestamp))
    }
    println()
  }

  private def printCommands() {
    printLogo()
    println("\n\nUsage: lime-submit [<spark-args> --] <lime-args> [-version]")
    println("\nChoose one of the following commands:")
    println()
    commands.foreach(cmd => {
      println("%20s : %s".format(cmd.commandName, cmd.commandDescription))
    })
    println()
  }

  def run() {
    log.info("Lime invoked with args: %s".format(argsToString(args)))
    if (args.length < 1) {
      printCommands()
    } else if (args.contains("--version") || args.contains("-version")) {
      printVersion()
    } else {

      args.headOption
        .flatMap(cmdName => {
          commands.find(_.commandName == cmdName)
        }).fold({
          printCommands()
        })(cmd => {
          cmd.apply(args.tail).run()
        })
    }
  }

  // Attempts to format the `args` array into a string in a way
  // suitable for copying and pasting back into the shell.
  private def argsToString(args: Array[String]): String = {
    def escapeArg(s: String) = "\"" + s.replaceAll("\\\"", "\\\\\"") + "\""
    args.map(escapeArg).mkString(" ")
  }
}
