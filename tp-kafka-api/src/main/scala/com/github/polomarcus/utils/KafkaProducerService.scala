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
      val metadata = producer.send(record) // @TODO
      logger.info(s"""
        Sending message with key "$key" and value "$value"
        Topic : ${topic}
        Boostrap server : ${ConfService.BOOTSTRAP_SERVERS_CONFIG}
        Offset : ${metadata.get().offset()}
      """)
    } catch {
      case e:Exception => logger.error(e.toString)
    }
  }

  def flush() = {
    producer.flush()
  }

  def close() = {
    producer.close()
  }
}
