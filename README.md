# Iterator Failures

This repo demonstrates the fact that if an iterator fails during user-initiated compactions, the fate persists, even though it will never work.


## Usage

Make sure you're using java 8:

```
java -version
```

Then run:

```
git checkout https://github.com/loganasherjones/accumulo-iterators-failures
cd accumulo-iterator-failures
./gradlew build
```

This will run the [test](src/test/kotlin/TestErrorOnNextIterator.kt) which should fail.

Here is what the test does

1. Spins up Accumulo using the `MiniAccumuloCluster`
2. Creates a table
3. Inserts a row into the table
4. Issues a compaction
5. Validates that the compaction is created and succeeds
6. Adds an iterator to the table that throws an error on the `next` call
7. Validates that the iterator is finished being set
8. Issues another compaction on the table
9. This causes an issue in which the tablet server is continuously trying to compact, but can't. The fate stays `IN_PROGRESS` and never goes to `FAILED` and so the test fails.

