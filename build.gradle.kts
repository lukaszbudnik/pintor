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
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.8.0")
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