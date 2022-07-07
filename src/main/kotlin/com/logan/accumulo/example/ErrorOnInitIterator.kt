package com.logan.accumulo.example

import org.apache.accumulo.core.data.ByteSequence
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.iterators.IteratorEnvironment
import org.apache.accumulo.core.iterators.SortedKeyValueIterator
import org.apache.accumulo.core.iterators.WrappingIterator
import org.slf4j.LoggerFactory

class ErrorOnInitIterator: WrappingIterator {
    private val logger = LoggerFactory.getLogger(ErrorOnInitIterator::class.java)

    constructor() { }

    constructor(other: ErrorOnInitIterator, env: IteratorEnvironment) {
        this.source = other.source.deepCopy(env)
    }

    override fun init(
        source: SortedKeyValueIterator<Key, Value>?,
        options: MutableMap<String, String>?,
        env: IteratorEnvironment?
    ) {
        super.init(source, options, env)
        logger.error("error initializing the iterator...")
        throw IllegalArgumentException("Error on init")
    }

    override fun next() {
        logger.debug("Next called on error iterator")
        super.next()
    }

    override fun seek(range: Range?, columnFamilies: MutableCollection<ByteSequence>?, inclusive: Boolean) {
        logger.debug("Seek called on error iterator")
        super.seek(range, columnFamilies, inclusive)
    }

    override fun deepCopy(env: IteratorEnvironment?): SortedKeyValueIterator<Key, Value> {
        return ErrorOnInitIterator(this, env!!)
    }

}