package helper

class OutputLogger {
  private val recordAccumulator = ContextHelper.getSparkContext.accumulator(0, "Record Count")
  def getRecordCount = recordAccumulator
  def incrementRecord = recordAccumulator += 1
}