package org.qcri.rheem.api.graph

import org.qcri.rheem.api._
import org.qcri.rheem.basic.data.Record
import org.qcri.rheem.basic.operators.{MapOperator, PageRankOperator}
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval

/**
 * 这个类通过[[Record]]s来增强[[DataQuanta]]的功能。
  * This class enhances the functionality of [[DataQuanta]] with [[Record]]s.
  */
class EdgeDataQuanta(dataQuanta: DataQuanta[Edge]) {

  implicit def planBuilder: PlanBuilder = dataQuanta.planBuilder

  /**
    * Feed this instance into a [[PageRankOperator]].
    *
    * @param numIterations number of PageRank iterations
    * @return a new instance representing the [[MapOperator]]'s output
    */
  def pageRank(numIterations: Int = 20,
               dampingFactor: Double = PageRankOperator.DEFAULT_DAMPING_FACTOR,
               graphDensity: ProbabilisticDoubleInterval = PageRankOperator.DEFAULT_GRAPH_DENSITIY):
  DataQuanta[PageRank] = {
    val pageRankOperator = new PageRankOperator(numIterations, dampingFactor, graphDensity)
    dataQuanta.connectTo(pageRankOperator, 0)
    wrap[PageRank](pageRankOperator)
  }

}