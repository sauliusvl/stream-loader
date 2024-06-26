/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.file

import java.time.Duration

case class FileStats(fileOpenDuration: Duration, fileSize: Long, recordsWritten: Long)

/**
  * Trait for defining a strategy for completing a multi-file batch.
  */
trait MultiFileCommitStrategy {

  /**
    * Returns whether a file batch is complete given the stats of the files.
    */
  def shouldCommit(files: Seq[FileStats]): Boolean
}

object MultiFileCommitStrategy {

  /**
    * Builds a commit strategy that commits when any file satisfies the given single file commit strategy.
    */
  def anyFile(single: FileCommitStrategy): MultiFileCommitStrategy =
    (files: Seq[FileStats]) =>
      files.exists(fs => single.shouldCommit(fs.fileOpenDuration, fs.fileSize, fs.recordsWritten))

  /**
    * Builds a commit strategy that commits when all files satisfy the given single file commit strategy.
    */
  def allFiles(single: FileCommitStrategy): MultiFileCommitStrategy =
    (files: Seq[FileStats]) =>
      files.forall(fs => single.shouldCommit(fs.fileOpenDuration, fs.fileSize, fs.recordsWritten))

  /**
    * Builds a commit strategy that commits when the total file stats (i.e. total file size, total record count
    * and max open time) satisfy the given single file commit strategy.
    */
  def total(single: FileCommitStrategy): MultiFileCommitStrategy = { (files: Seq[FileStats]) =>
    {
      if (files.isEmpty) {
        false
      } else {
        single.shouldCommit(
          files.maxBy(_.fileOpenDuration).fileOpenDuration,
          files.map(_.fileSize).sum,
          files.map(_.recordsWritten).sum
        )
      }
    }
  }
}
