import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.util.MonitorUtil
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl
import org.apache.accumulo.monitor.Monitor
import org.apache.zookeeper.KeeperException
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory
import kotlin.io.path.createTempDirectory

object MacUtils {

    private val logger = LoggerFactory.getLogger(MacUtils::class.java)

    private val tempDir by lazy {
        createTempDirectory("mac-").toFile()
    }

    val zkInstance by lazy {
        ZooKeeperInstance(cluster.clientConfig)
    }

    val connector by lazy {
        zkInstance.getConnector("root", PasswordToken(rootPassword))
    }

    val instanceName = "instance"
    val zookeeperPort = 2181
    private val rootPassword = "password"
    private const val jdwpEnabled = false
    private const val monitorPort = 9995
    private const val tServerClientPort = 9997
    private const val numTServers = 2

    val config by lazy {
        val config = MiniAccumuloConfigImpl(tempDir, rootPassword)
        val siteConfig = config.siteConfig
        siteConfig[Property.MONITOR_PORT.key] = monitorPort.toString()
        siteConfig[Property.TSERV_CLIENTPORT.key] = tServerClientPort.toString()
        config.siteConfig = siteConfig

        config.instanceName = instanceName
        config.numTservers = numTServers
        config.zooKeeperPort = zookeeperPort
        config.isJDWPEnabled = jdwpEnabled
        config
    }

    val cluster by lazy {
        MiniAccumuloClusterImpl(config)
    }

    fun stopCluster() {
        cluster.stop()
    }

    fun startCluster() {
        logger.info("Starting cluster. You can see the logs for the cluster here: $tempDir")
        cluster.start()
        cluster.exec(Monitor::class.java)
        val monitorStatus = retryIO(times = 5, initialDelay = 1000, maxDelay = 5000) { checkMonitor() }
        if (!monitorStatus.first) {
            fail("The monitor did not start.")
        }
        logger.info("The monitor started, now waiting for all other processes to start...")
        while (true) {
            if (areAllProcessesAlive()) {
                break
            }
        }
        logger.info("Cluster successfully started...")

        Runtime.getRuntime().addShutdownHook(object: Thread() {
            override fun run() {
                cluster.stop()
            }
        })
    }

    private fun areAllProcessesAlive(): Boolean {
        return cluster.processes.values.all {
            it.all { p -> p.process.isAlive }
        }
    }

    private fun checkMonitor(): Pair<Boolean, String> {
        try {
            val instance = ZooKeeperInstance(cluster.clientConfig)
            val monitorLocation = MonitorUtil.getLocation(instance)
            if (monitorLocation !== null) {
                return Pair(true, monitorLocation)
            }
        } catch (e: KeeperException) {
            return Pair(false, "")
        }
        return Pair(false, "")
    }


}