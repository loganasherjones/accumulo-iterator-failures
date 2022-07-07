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

