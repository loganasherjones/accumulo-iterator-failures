package com.logan.accumulo.example

import java.lang.Thread.sleep
import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.iterators.WrappingIterator
import org.slf4j.LoggerFactory

class ErrorOnNextIterator: WrappingIterator {
    private val logger = LoggerFactory.getLogger(ErrorOnNextIterator::class.java)

    constructor() : super() {
        logger.warn("ErrorOnNext default constructor called")
    }

    constructor(other: ErrorOnNextIterator, env: IteratorEnvironment) {
        logger.warn("ErrorOnNext copy constructor called")
        this.source = other.source.deepCopy(env)
    }

    override fun init(
        source: SortedKeyValueIterator<Key, Value>?,
        options: MutableMap<String, String>?,
        env: IteratorEnvironment?
    ) {
        logger.warn("ErrorOnNext initialization called")
        super.init(source, options, env)
    }

    override fun next() {
        logger.warn("Next called on ErrorOnNextIterator, in 5 seconds, I will error.")
        sleep(5)
        throw IllegalArgumentException("Cannot do next.")
    }

    override fun seek(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
        logger.warn("Seek called on ErrorOnNextIterator, in 5 seconds, I will error.")
        super.seek(range, columnFamilies, inclusive)
    }

    override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key, Value> {
        logger.warn("ErrorOnNext, deepCopy called")
        return ErrorOnNextIterator(this, env!!)
    }

}