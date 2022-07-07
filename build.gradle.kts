plugins {
    base
    java
    kotlin("jvm") version "1.6.21"
    idea
    `java-library`
}

description = "Example iterators that throw errors"
version = "0.0.1"

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
        kotlinOptions.allWarningsAsErrors = true
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = JavaVersion.VERSION_1_8.toString()
        kotlinOptions.allWarningsAsErrors = true
    }
    test {
        useJUnitPlatform()
        testLogging {
            events("skipped", "failed")
            showStandardStreams = true
        }
    }
}

repositories {
    mavenCentral()
}

val accumuloVersion = "1.10.2"
val slf4jVersion = "1.7.36"
val logbackVersion = "1.2.11"

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", "1.6.1")
    implementation("org.apache.accumulo", "accumulo-core", accumuloVersion)


    testImplementation("org.apache.accumulo", "accumulo-minicluster", accumuloVersion)
    testImplementation(platform("org.junit:junit-bom:5.8.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}
