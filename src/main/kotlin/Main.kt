import io.prometheus.client.Gauge
import io.prometheus.client.Histogram
import io.prometheus.client.exporter.HTTPServer
import java.io.File
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.random.Random

var stop = false
var sensors_stop = false

val sensors_count = Gauge.build().name("count_of_sensors").help("Count of sensors").register()
val sensors_queue = Gauge.build().name("sensor_queue_len").help("Len of sensors queue").register()
val logs_queue = Gauge.build().name("logs_queue_len").help("Len of logs queue").register()
val sensors_time = Histogram.build().name("sensors_time").help("sensors time").register()

fun main() {
    val server = HTTPServer.Builder().withPort(1234).build()
    Thread.sleep(1000)
    val loggingQueue = ArrayBlockingQueue<LoggerData>(1000, true)
    val sensorQueue = ArrayBlockingQueue<SensorData>(1000, true)

    Logger(loggingQueue).start()
    Host(sensorQueue, loggingQueue).start()
    runSensors(1000,sensorQueue)

//    Thread.sleep(5000)//work for sum time
    waitForQueue(loggingQueue, sensorQueue)
    waitForSensors()
    stopAll(loggingQueue, sensorQueue)
    Thread.sleep(100)
    server.close()
    println("end of main")
}

private fun stopAll(loggingQueue: ArrayBlockingQueue<LoggerData>, sensorQueue: ArrayBlockingQueue<SensorData>) {
    sensors_stop = true
    waitForQueue(loggingQueue, sensorQueue)
    stop = true
}

private fun waitForQueue(
    loggingQueue: ArrayBlockingQueue<LoggerData>,
    sensorQueue: ArrayBlockingQueue<SensorData>
) {
    while (loggingQueue.isNotEmpty() || sensorQueue.isNotEmpty()) {
        println("Waiting for queue to empty: loggingQueue=${loggingQueue.count()}  sensorQueue=${sensorQueue.count()}")
        Thread.sleep(1000)
    }
}

private fun waitForSensors(){
    while (sensors_count.get()>0){
    }
}

private fun runSensors(count:Int, queue: ArrayBlockingQueue<SensorData>, isDeamon:Boolean=true, maxAlive:Int=500){
    var iter=0
    while (iter<=count) {
        if (sensors_count.get()>maxAlive){
            continue
        }
        val ts = Sensor(queue, iter, Random.nextLong(1, 1000), Random.nextInt(1, 100))
        ts.isDaemon = isDeamon
        ts.start()
        iter+=1
        Thread.sleep(1)
    }
}


class Logger(lq: ArrayBlockingQueue<LoggerData>) : Thread() {
    val loggingQueue = lq
    val sdf = SimpleDateFormat("dd/M/yyyy hh:mm:ss")
    val file = File("log.txt").bufferedWriter()
    override fun run() {
        name = "LoggingThread"
        var lg: LoggerData
        var logMassege: String
        while (!stop) {
            if (loggingQueue.isEmpty()) {
                continue
            }
            lg = loggingQueue.take()
            logs_queue.dec()
            lg.freeze()
            logMassege =
                "${lg.createLog(loggingQueue)} || ${sdf.format(Date())}\n"
            print(logMassege)
            file.write(logMassege)
            Thread.sleep(1)//to make logger work slow
        }
        file.close()
        println("Terning off logger")
    }
}

class Host(sq: ArrayBlockingQueue<SensorData>, lq: ArrayBlockingQueue<LoggerData>) : Thread() {
    val sensorQueue = sq
    val loggingQueue = lq
    override fun run() {
        name = "HostThread"
        var sd: SensorData
        var lg: LoggerData
        while (!stop) {
            if (sensorQueue.isEmpty()) {
                continue
            }
            sd = sensorQueue.take()
            sensors_queue.dec()
            sd.freeze()
            lg = LoggerData(sd, sensorQueue.count())
            loggingQueue.put(lg)
            logs_queue.inc()
        }
        println("Terning off host")
    }
}

class Sensor(sq: ArrayBlockingQueue<SensorData>, id: Int, ping: Long = 1000, lifecicle:Int=-1) : Thread() {
    val sensorQueue = sq
    val id = id
    val p = ping
    var lc=lifecicle
    override fun run() {
        sensors_count.inc()
        name = "Sensor:$id"
        while (!sensors_stop && lc!=0) {
            sensorQueue.put(SensorData(id, Random.nextInt(1, 100)))
            sensors_queue.inc()
            Thread.sleep(p)
            lc-=1
        }
        sensors_count.dec()
        println("Terning off sensor:$id")
    }
}




open class ThreadData(val timestamp: Long = System.currentTimeMillis()) {
    var freezeTime: Long = -1
    open fun freeze() {
        freezeTime = System.currentTimeMillis() - timestamp
    }
}
data class SensorData(val id: Int, val temp: Int) : ThreadData(){
    val timest=sensors_time.startTimer()
    override fun freeze() {
        timest.observeDuration()
        super.freeze()
    }
}
data class LoggerData(val sensorData: SensorData, val sensorLen: Int) : ThreadData() {
    fun createLog(lq:ArrayBlockingQueue<LoggerData>): String =
        "Sensor id ${sensorData.id} send value=${sensorData.temp} s_time=${sensorData.freezeTime} sensorQueue=${sensorLen} | l_time=${freezeTime} loggingQueue=${lq.count()}"
}
