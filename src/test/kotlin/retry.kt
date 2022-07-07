fun <T> retryIO(
    times: Int = Int.MAX_VALUE,
    initialDelay: Long = 100, // 0.1 second
    maxDelay: Long = 1000,    // 1 second
    factor: Double = 2.0,
    block: () -> Pair<Boolean, T>
): Pair<Boolean, T> {
    var currentDelay = initialDelay
    repeat(times - 1) {
        val result = block()
        if (result.first) {
            return result
        }
        Thread.sleep(currentDelay)
        currentDelay = (currentDelay * factor).toLong().coerceAtLeast(maxDelay)
    }
    return block()
}