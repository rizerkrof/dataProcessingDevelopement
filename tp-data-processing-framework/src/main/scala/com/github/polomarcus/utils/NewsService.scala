package com.github.polomarcus.utils

import com.typesafe.scalalogging.Logger
import com.github.polomarcus.model.News
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, to_timestamp}

import scala.util.matching.Regex

object NewsService {
  val logger = Logger(NewsService.getClass)

  val spark = SparkService.getAndConfigureSparkSession()
  import spark.implicits._

  def read(path: String) = {
    spark.read.json(path).withColumn("date", to_timestamp(col("date"))).as[News]
  }

  /**
   * Apply ClimateService.isClimateRelated function to see if a news is climate related
   * @param newsDataset
   * @return
   */
  def enrichNewsWithClimateMetadata(newsDataset: Dataset[News]) : Dataset[News] = {
    newsDataset.map { news =>
      val enrichedNews = News(
        news.title,
        news.description,
        news.date,
        news.order,
        news.presenter,
        news.authors,
        news.editor,
        news.editorDeputy,
        news.url,
        news.urlTvNews,
        ClimateService.isClimateRelated(news.title + news.description), // @TODO: we need to apply a function here from ClimateService
        news.media
      )

      enrichedNews
    }
  }

  /**
   * Only keep news about climate
   *
   * Tips --> https://alvinalexander.com/scala/how-to-use-filter-method-scala-collections-cookbook/
   *
   * @param newsDataset
   * @return newsDataset but with containsWordGlobalWarming to true
   */
  def filterNews(newsDataset: Dataset[News]) : Dataset[News] = {
    newsDataset.filter { news => news.containsWordGlobalWarming }
  }

  /**
   * count the number of news that is climate change related
   * @param dataset of news
   * @return a count
   */
  def getNumberOfNewsClimateChangeRelated(dataset: Dataset[News]): Long = {
    //@TODO look a the Spark API to know how to count
    filterNews(dataset).count()
  }

  /**
   * count the number of news
   * @param dataset of news
   * @return a count
   */
  def getNumberOfNews(dataset: Dataset[News]): Long = {
    //@TODO look a the Spark API to know how to count
    dataset.count()
  }

}
