package model

class TableList(bt: BaseTableInfo, sl: List[SourceTableInfo]) {
  def baseTableInfo = bt
  def sourceTableInfoList = sl
}