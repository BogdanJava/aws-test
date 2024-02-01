package by.bahdan.awskinesisapp.service

import by.bahdan.awskinesisapp.sampledata.IoTEvent
import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamResult
import com.amazonaws.services.kinesis.model.GetRecordsRequest
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.amazonaws.services.kinesis.model.Shard
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.ByteBuffer


class KinesisStreamService(
    private val kinesisClient: AmazonKinesis,
    private val streamName: String
) {

    init {
        validateStream()
    }

    private val mapper = ObjectMapper()

    fun readFromSequenceNumber(sequenceNumber: String) {
        val lastShardId = getLastShardId()
        val iteratorRequest = GetShardIteratorRequest().also {
            it.streamName = streamName
            it.shardIteratorType = "AT_SEQUENCE_NUMBER"
            it.startingSequenceNumber = sequenceNumber
            it.shardId = lastShardId
        }

        val shardIteratorResult = kinesisClient.getShardIterator(iteratorRequest)
        val shardIterator = shardIteratorResult.shardIterator

        readWithIterator(shardIterator)
    }

    private fun readWithIterator(shardIterator: String) {
        val recordsRequest = GetRecordsRequest().also {
            it.shardIterator = shardIterator
            it.limit = 1000
        }

        val result = kinesisClient.getRecords(recordsRequest)
        val records = result.records

        records.forEach {
            println("Record read from stream [$streamName]: ${String(it.data.array())}")
        }
    }

    fun readStream() {
        val lastShardId = getLastShardId()

        val iteratorRequest = GetShardIteratorRequest().also {
            it.streamName = streamName
            it.shardIteratorType = "TRIM_HORIZON"
            it.shardId = lastShardId
        }

        val shardIteratorResult = kinesisClient.getShardIterator(iteratorRequest)
        val shardIterator = shardIteratorResult.shardIterator

        readWithIterator(shardIterator)
    }

    private fun getLastShardId(): String? {
        val shards = ArrayList<Shard>()
        var lastShardId: String? = null
        var streamDescription: DescribeStreamResult

        do {
            streamDescription = kinesisClient.describeStream(streamName)
            shards.addAll(streamDescription.streamDescription.shards)

            if (shards.size > 0) {
                lastShardId = shards.get(shards.size - 1).shardId
            }
        } while (streamDescription.streamDescription.hasMoreShards)
        return lastShardId
    }

    fun sendEvent(event: IoTEvent) = try {
        val bytes = mapper.writeValueAsBytes(event)

        val request = PutRecordRequest().also {
            it.partitionKey = event.deviceId
            it.streamName = streamName
            it.data = ByteBuffer.wrap(bytes)
        }

        val response = kinesisClient.putRecord(request)
        println("Data sent to kinesis stream. Sequence Number: ${response.sequenceNumber}")
    } catch (e: Exception) {
        println("Error sending event to kinesis stream: ${e.message}")
    }

    private fun validateStream() {
        val request = DescribeStreamRequest().also {
            it.streamName = streamName
        }
        val response = kinesisClient.describeStream(request)

        if (response.streamDescription.streamStatus.toString() != "ACTIVE") {
            throw RuntimeException("Stream not active")
        }
    }
}