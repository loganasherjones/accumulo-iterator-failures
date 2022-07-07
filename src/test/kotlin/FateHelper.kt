import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.zookeeper.ZooUtil
import org.apache.accumulo.fate.AdminUtil
import org.apache.accumulo.fate.ReadOnlyTStore
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter
import org.apache.accumulo.fate.ZooStore
import org.apache.accumulo.shell.commands.FateCommand
import org.slf4j.LoggerFactory

class FateHelper(conn: Connector) {

    private val logger = LoggerFactory.getLogger(FateHelper::class.java)

    private val scheme = "digest"
    private val systemUser = "accumulo"
    private val siteSecret = "DONTTELL"
    private val inst = conn.instance
    private val rootInst = ZooUtil.getRoot(inst)
    private val admin = AdminUtil<FateCommand>(false)
    private val path = "${rootInst}${Constants.ZFATE}"
    private val zTableLocks = "${rootInst}${Constants.ZTABLE_LOCKS}"
    private val authBytes = "$systemUser:$siteSecret".toByteArray()
    private val zk = ZooReaderWriter(conn.instance.zooKeepers, conn.instance.zooKeepersSessionTimeOut, scheme, authBytes)
    private val zs = ZooStore<FateCommand>(path, zk)

    private val numTries = 5
    private val initialDelay = 1_000L

    fun waitForAllTransactionsToComplete(): Triple<Boolean, Set<String>, Set<String>> {
        logger.info("Waiting for all transaction(s) to complete...")
        val (hasTransactions, transactions) = retryIO(times = numTries, initialDelay = initialDelay) {
            val txs = getAllTransactions()
            Pair(txs.isNotEmpty(), txs)
        }
        if (!hasTransactions) {
            return Triple(false, emptySet(), emptySet())
        }
        return waitForTransactionsToComplete(transactions.map { it.txid }.toSet())
    }

    fun waitForTransactionsToComplete(txIds: Set<String>): Triple<Boolean, Set<String>, Set<String>> {
        val txIdLongs = txIds.map { java.lang.Long.parseLong(it, 16) }.toSet()
        logger.info("Waiting for $txIds to complete")

        var completedTxIds = emptySet<String>()
        val (isComplete, runningTxIds) = retryIO(times = numTries, initialDelay = initialDelay) {
            val stillRunningTxIds = getAllTransactions(txIdLongs).filter { it.status != ReadOnlyTStore.TStatus.SUCCESSFUL }.map { it.txid }.toSet()
            completedTxIds = txIds subtract stillRunningTxIds
            Pair(completedTxIds.size == txIds.size, stillRunningTxIds)
        }

        if (isComplete) {
            logger.debug("All transactions ($txIds) finished")
        } else {
            logger.debug("${completedTxIds.size} transactions completed: $completedTxIds")
            logger.debug("${runningTxIds.size} transactions still running: $runningTxIds")
        }

        return Triple(isComplete, completedTxIds, runningTxIds)
    }

    private fun getAllTransactions(txIds: Set<Long>? = null): List<AdminUtil.TransactionStatus> {
        val status = admin.getStatus(zs, zk, zTableLocks, txIds?.toMutableSet(), null)
        logger.debug("Got ${status.transactions.size} transactions")
        return status.transactions
    }
}