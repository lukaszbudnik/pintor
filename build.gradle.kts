plugins {
    id("java")
    id("jacoco")
}

group = "com.github.lukaszbudnik"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")
    
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.8.0")
    testImplementation("ch.qos.logback:logback-classic:1.5.13")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("performance")
    }
}

// Performance test task - runs separately from unit tests
tasks.register<Test>("performanceTest") {
    description = "Runs performance tests"
    group = "verification"
    
    useJUnitPlatform {
        includeTags("performance")
    }
    
    // Give performance tests more memory and time
    maxHeapSize = "2g"
    
    // Show detailed output for performance tests
    testLogging {
        events("passed", "skipped", "failed", "standard_out", "standard_error")
        showStandardStreams = true
    }
}