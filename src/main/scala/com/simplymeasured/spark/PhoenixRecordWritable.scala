/*
   Copyright 2014 Simply Measured, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.simplymeasured.spark

import java.io.{DataOutput, DataInput}
import java.sql.{PreparedStatement, ResultSet}

import org.apache.hadoop.mapreduce.lib.db.DBWritable
import org.apache.hadoop.io.Writable

import scala.collection.immutable
import scala.collection.mutable

class PhoenixRecordWritable extends DBWritable with Writable {
  val resultMap = mutable.Map[String, AnyRef]()

  def result : immutable.Map[String, AnyRef] = {
    resultMap.toMap
  }

  override def write(preparedStatement: PreparedStatement): Unit = {
    throw new UnsupportedOperationException("This is unsupported in this context")
  }

  override def readFields(resultSet: ResultSet): Unit = {
    val metadata = resultSet.getMetaData

    for(i <- 1 to metadata.getColumnCount) {
      resultMap(metadata.getColumnLabel(i)) = resultSet.getObject(i)
    }
  }

  override def write(dataOutput: DataOutput): Unit = {
    // intentionally non-op
  }

  override def readFields(dataInput: DataInput): Unit = {
    // intentionally non-op
  }
}
