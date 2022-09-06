package com.github.polomarcus.utils

import com.github.polomarcus.conf.ConfService
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer._
import org.sparkproject.jetty.util.ProcessorUtils

import java.util.Properties
import scala.Predef.Ensuring

object KafkaProducerService {
  val logger = Logger(KafkaProducerService.getClass)

  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfService.BOOTSTRAP_SERVERS_CONFIG)

  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")

  private val producer = new KafkaProducer[String, String](props)

  def produce(topic: String, key: String, value: String): Unit = {
    val record = new ProducerRecord(topic, key, value)

    try {
      logger.info(s"topic : ${topic}")
      logger.info(s"bootstrap server : ${ConfService.BOOTSTRAP_SERVERS_CONFIG}")
      // val topics =
      // logger.info(s"${producer.listTopics()}")
      // producer.beginTransaction()
      val metadata = producer.send(record) // @TODO
      // producer.commitTransaction()
      // print(metadata)
      logger.info(s"""
        Sending message with key "$key" and value "$value"
        Offset : ${metadata.get().offset()}
      """)
    } catch {
      case e:Exception => logger.error(e.toString)
    } finally { // --> "finally" happens everytime and the end, even if there is an error
      //@see on why using flush : https://github.com/confluentinc/confluent-kafka-python/issues/137#issuecomment-282427382
      //@TODO to speed up this function that send one message at the time, what could we do ?
      // producer.flush()
    }
  }

  def flush() = {
    producer.flush()
  }

  def close() = {
    producer.close()
  }
}
