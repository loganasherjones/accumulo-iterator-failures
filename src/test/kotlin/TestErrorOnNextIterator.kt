import com.logan.accumulo.example.ErrorOnNextIterator
import java.util.concurrent.TimeUnit
import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.admin.CompactionConfig
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.zookeeper.ZooUtil
import org.apache.accumulo.fate.AdminUtil
import org.apache.accumulo.fate.ReadOnlyTStore
import org.apache.accumulo.fate.ZooStore
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter
import org.apache.accumulo.shell.commands.FateCommand
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory

class TestErrorOnNextIterator {
    private val scheme = "digest"
    private val systemUser = "accumulo"
    private val siteSecret = "DONTTELL"

    private val logger = LoggerFactory.getLogger(TestErrorOnNextIterator::class.java)

    @Test
    fun testIteratorFailures() {
        MacUtils.startCluster()

        val tableName = "error_on_next_table"
        val conn = MacUtils.connector

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

        logger.info("Running a normal compaction on $tableName")
        val compactConfig = CompactionConfig().apply {
            startRow = null
            endRow = null
            flush = true
            wait = false
        }
        tableOps.compact(tableName, compactConfig)
        logger.info("Running a compaction on $tableName...")
        verifyTransactionCompletes(conn)

        // Add The ErrorOnNextIterator to the table.
        logger.info("Attaching the ErrorOnNextIterator to $tableName")
        val nextIteratorSetting = IteratorSetting(10, ErrorOnNextIterator::class.java)
        tableOps.attachIterator(tableName, nextIteratorSetting)
        verifyTransactionCompletes(conn)
        logger.info("Iterator is attached!")

        logger.info("Running the compaction again on $tableName")
        tableOps.compact(tableName, compactConfig)
        verifyTransactionCompletes(conn)

        MacUtils.stopCluster()
    }

    private fun verifyTransactionCompletes(conn: Connector) {
        val inst = conn.instance
        val rootInst = ZooUtil.getRoot(inst)
        val admin = AdminUtil<FateCommand>(false)
        val path = "${rootInst}${Constants.ZFATE}"
        val zTableLocks = "${rootInst}${Constants.ZTABLE_LOCKS}"
        val authBytes = "$systemUser:$siteSecret".toByteArray()
        val zk = ZooReaderWriter(conn.instance.zooKeepers, conn.instance.zooKeepersSessionTimeOut, scheme, authBytes)
        val zs = ZooStore<FateCommand>(path, zk)

        logger.debug("Getting a list of transactions...")
        val (haveTransactions, transactions) = retryIO(times = 10, initialDelay = 1000, maxDelay = 1000) {
            val status = admin.getStatus(zs, zk, zTableLocks, null, null)
            logger.debug("Got ${status.transactions.size} transactions...")
            Pair(status.transactions.isNotEmpty(), status.transactions)
        }
        if (!haveTransactions) {
            fail("Did not find any fate transactions")
        }
        assertEquals(1, transactions.size)
        val tx = transactions.first()
        logger.info("Verifying that ${tx.txid} completes...")

        val txId = java.lang.Long.parseLong(tx.txid, 16)
        val (isComplete, _) = retryIO(times = 10, initialDelay = 1000, maxDelay = 1000) {
            val status = admin.getStatus(zs, zk, zTableLocks, setOf(txId), null)
            logger.debug("Got ${status.transactions.size} transactions...")
            if (status.transactions.isEmpty()) {
                Pair(true, null)
            } else {
                assertEquals(1, status.transactions.size)
                val t = status.transactions.first()
                assertEquals(tx.txid, t.txid)
                logger.debug("Status: ${t.status}")
                when (t.status) {
                    ReadOnlyTStore.TStatus.SUCCESSFUL -> Pair(true, null)
                    else -> Pair(false, null)
                }
            }
        }
        if (isComplete) {
            logger.info("Transaction ${tx.txid} finished as expected")
            return
        }
        fail("Transaction ${tx.txid} didn't finish in time")
    }
}