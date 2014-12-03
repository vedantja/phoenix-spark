package com.simplymeasured.spark

protected class PhoenixTable(pc: PhoenixContext) {

}

object PhoenixTable {
  def apply(pc: PhoenixContext, tableName: String): PhoenixTable = {
    new PhoenixTable(pc)
  }
}