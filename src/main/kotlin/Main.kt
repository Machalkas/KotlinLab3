import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.random.Random

var stop = false
var sensors_stop = false

fun main() {
    Thread.sleep(10000)
    val loggingQueue = ArrayBlockingQueue<LoggerData>(1000, true)
    val sensorQueue = ArrayBlockingQueue<SensorData>(1000, true)
    Logging(loggingQueue).start()
    Host(sensorQueue, loggingQueue).start()
    for (i in 1..1000) {
        val x = TempSensor(sensorQueue, i, Random.nextLong(1, 100))
        x.isDaemon = true
        x.start()
    }
    Thread.sleep(5000)
    sensors_stop = true
    while (loggingQueue.isNotEmpty() || sensorQueue.isNotEmpty()) {
        println("Waiting for queue to empty: loggingQueue=${loggingQueue.count()}  sensorQueue=${sensorQueue.count()}")
        Thread.sleep(1000)
    }
    stop = true
    println("end of main")
}

class Logging(lq: ArrayBlockingQueue<LoggerData>) : Thread() {
    val loggingQueue = lq
    val sdf = SimpleDateFormat("dd/M/yyyy hh:mm:ss")
    val file = File("log.txt").bufferedWriter()
    public override fun run() {
        name = "LoggingThread"
        var lg: LoggerData
        var logMassege: String
        while (!stop) {
            if (loggingQueue.isEmpty()) {
                continue
            }
            lg = loggingQueue.take()
            lg.frezzee()
            logMassege =
                "${lg.createLog()}time=${lg.freezzeeTime} loggingQueue=${loggingQueue.count()} || ${sdf.format(Date())}\n"
            print(logMassege)
            file.write(logMassege)
            Thread.sleep(1)
        }
        file.close()
        println("Terning off logger")
    }
}

class Host(sq: ArrayBlockingQueue<SensorData>, lq: ArrayBlockingQueue<LoggerData>) : Thread() {
    val sensorQueue = sq
    val loggingQueue = lq
    public override fun run() {
        name = "HostThread"
        var sd: SensorData
        var lg: LoggerData
        while (!stop) {
            if (sensorQueue.isEmpty()) {
                continue
            }
            sd = sensorQueue.take()
            sd.frezzee()
            lg = LoggerData(sd, sensorQueue.count())
            loggingQueue.put(lg)
        }
        println("Terning off host")
    }
}

class TempSensor(sq: ArrayBlockingQueue<SensorData>, id: Int, ping: Long = 1000) : Thread() {
    val sensorQueue = sq
    val id = id
    val p = ping
    public override fun run() {
        name = "Sensor:$id"
        while (!sensors_stop) {
            sensorQueue.put(SensorData(id, Random.nextInt(1, 100)))
            Thread.sleep(p)
        }
        println("Terning off sensor:$id")
    }
}




open class ThreadData(val timestamp: Long = System.currentTimeMillis()) {
    var freezzeeTime: Long = -1
    fun frezzee() {
        freezzeeTime = System.currentTimeMillis() - timestamp
    }
}
data class SensorData(val id: Int, val temp: Int) : ThreadData()
data class LoggerData(val sensorData: SensorData, val sensorLen: Int) : ThreadData() {
    fun createLog(): String =
        "Receive sensor=${sensorData.id} value=${sensorData.temp} time=${sensorData.freezzeeTime} sensorQueue=${sensorLen} | "
}
