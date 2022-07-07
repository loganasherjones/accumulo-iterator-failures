import com.logan.accumulo.example.ErrorOnNextIterator
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.CompactionConfig
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory

class TestErrorOnNextIterator {

    private val logger = LoggerFactory.getLogger(TestErrorOnNextIterator::class.java)

    @Test
    fun testIteratorFailures() {
        MacUtils.startCluster()

        val tableName = "error_on_next_table"
        val conn = MacUtils.connector
        val fateHelper = FateHelper(conn)

        logger.info("Creating $tableName")
        val tableOps = conn.tableOperations()
        tableOps.create(tableName)

        logger.info("Writing data to $tableName...")
        val m = Mutation("row1")
        m.put("cf", "cq", Value("value1"))
        val writerConfig = BatchWriterConfig().setMaxMemory(1_000_000).setMaxLatency(1_000, TimeUnit.MILLISECONDS).setMaxWriteThreads(10)
        val writer = conn.createBatchWriter(tableName, writerConfig)
        writer.addMutation(m)
        writer.flush()

        val compactConfig = CompactionConfig().apply {
            startRow = null
            endRow = null
            flush = true
            wait = false
        }

        logger.info("Running a normal compaction on $tableName")
        tableOps.compact(tableName, compactConfig)
        if (!fateHelper.waitForAllTransactionsToComplete().first) {
            fail("Initial compaction on $tableName never completed")
        }
        logger.info("Compaction completed successfully as expected")

        // Add The ErrorOnNextIterator to the table.
        logger.info("Attaching the ErrorOnNextIterator to $tableName")
        val nextIteratorSetting = IteratorSetting(10, ErrorOnNextIterator::class.java)
        tableOps.attachIterator(tableName, nextIteratorSetting)
        if (!fateHelper.waitForAllTransactionsToComplete().first) {
            fail("Failed to attached iterator to $tableName")
        }
        logger.info("Iterator is attached!")

        logger.info("Running the compaction again on $tableName")
        tableOps.compact(tableName, compactConfig)
        val (allCompleted, completedTxIds, runningTxIds) = fateHelper.waitForAllTransactionsToComplete()
        if (allCompleted) {
            fail("Somehow the compaction with the iterator that errors completed.")
        }
        assertEquals(0, completedTxIds.size)
        assertEquals(1, runningTxIds.size)
        logger.info("Compaction appears to be in crash loop txid: ${runningTxIds.first()}")

        // Optional, if you want to see what's happening in zookeeper at this point
        pause()

        logger.info("Canceling compactions on $tableName")
        tableOps.cancelCompaction(tableName)
        val (compactionCompleted, _, _) = fateHelper.waitForTransactionsToComplete(runningTxIds)
        assertTrue(compactionCompleted, "Compaction was not cleaned up after cancel")

        // Optional, if you want to see what's happening in zookeeper at thsi point
        pause()

        MacUtils.stopCluster()
    }

    private fun pause(numMinutes: Int = 5) {
        logger.info("Pausing for $numMinutes minutes. Have a look in ZooKeeper")
        runBlocking {
            launch {
                val numSeconds = numMinutes * 60
                val numMillis = (numSeconds * 1000).toLong()
                delay(numMillis)
            }
        }
    }
}