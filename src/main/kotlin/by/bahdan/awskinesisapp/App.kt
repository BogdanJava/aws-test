package by.bahdan.awskinesisapp

import by.bahdan.awskinesisapp.sampledata.EventEmissionInterval
import by.bahdan.awskinesisapp.sampledata.IoTEvent
import by.bahdan.awskinesisapp.sampledata.IoTSampleDataGenerator
import by.bahdan.awskinesisapp.service.KinesisStreamService
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import java.time.Duration
import java.util.concurrent.TimeUnit

fun main() {
    val kinesisClient = AmazonKinesisClientBuilder.standard().withRegion("ap-northeast-1").build()
    val streamName = "iot-dummy-data"
    val kinesisService = KinesisStreamService(kinesisClient, streamName)
    val eventsInterval = EventEmissionInterval(Duration.ofSeconds(1), Duration.ofSeconds(3))

    val dataGenerator = IoTSampleDataGenerator(2, eventsInterval) { event ->
        writeToKinesis(kinesisService, event)
    }.also {
        gracefulShutdownHook(it)
    }

    try {
        kinesisService.readFromSequenceNumber("49648855044255014847292553350165917504078022641297915906")
//        dataGenerator.startGenerating()
//        Thread.sleep(Duration.ofSeconds(10))
//        kinesisService.readStream()
//        keepRunning()
    } finally {
        dataGenerator.shutdown()
        kinesisClient.shutdown()
    }
}

fun keepRunning() {
    println("Running...")
    while (true) {
        TimeUnit.SECONDS.sleep(1)
    }
}

fun gracefulShutdownHook(dataGenerator: IoTSampleDataGenerator) {

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Shutting down gracefully...")
        dataGenerator.shutdown()
    })
}

fun writeToKinesis(kinesisService: KinesisStreamService, event: IoTEvent) {
    kinesisService.sendEvent(event)
}
