@file:Suppress("Since15")

package by.bahdan.awskinesisapp.sampledata

import java.time.Duration
import java.util.Date
import java.util.Random
import java.util.concurrent.Executors

/**
 * @param devices the number of devices
 * @param eventEmissionInterval
 */
class IoTSampleDataGenerator(
    private val devices: Int,
    private val eventEmissionInterval: EventEmissionInterval,
    private val eventCallback: (IoTEvent) -> Unit
) {
    private val executor = Executors.newFixedThreadPool(devices)

    fun startGenerating() {
        for (i in 0..devices) {
            executor.submit {
                val deviceId = "iot_device_id_$i"
                while (true) {
                    val event = IoTEvent(deviceId, IoTEventData.random())
                    eventCallback(event)
                    Thread.sleep(eventEmissionInterval.getRandomDurationBetween())
                }
            }
        }
    }

    fun shutdown() {
        if (!executor.isShutdown)
            executor.shutdownNow()
    }

}

data class IoTEvent(
    val deviceId: String,
    val data: IoTEventData
)

data class IoTEventData(
    val temperature: Double,
    val humidity: Double,
    val timestamp: Date
) {
    companion object {
        private val random = Random()

        fun random(): IoTEventData = IoTEventData(
            temperature = 15.0 + random.nextDouble() * 15.0,
            humidity = 40.0 + random.nextDouble() * 30.0,
            timestamp = Date()
        )
    }
}

class EventEmissionInterval {
    private val minWaitTime: Duration
    private val maxWaitTime: Duration

    private val random = Random()

    constructor(minWaitTime: Duration, maxWaitTime: Duration) {
        if (minWaitTime > maxWaitTime) throw IllegalArgumentException()

        this.minWaitTime = minWaitTime
        this.maxWaitTime = maxWaitTime
    }

    fun getRandomDurationBetween(): Duration {
        val randomMillis: Long = minWaitTime.toMillis() +
                (random.nextDouble() * (maxWaitTime.toMillis() - minWaitTime.toMillis())).toLong()
        return Duration.ofMillis(randomMillis)
    }
}