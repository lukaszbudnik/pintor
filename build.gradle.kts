plugins {
    id("java")
    id("jacoco")
    id("com.diffplug.spotless") version "7.2.1"
}

group = "com.github.lukaszbudnik"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

// Enforce Java 17
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Enforce Google Java Style
spotless {
    java {
        googleJavaFormat("1.28.0")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// Configure Jacoco
jacoco {
    toolVersion = "0.8.11"
}

dependencies {
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("io.projectreactor:reactor-core:3.7.6")

    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.19.0")
    testImplementation("ch.qos.logback:logback-classic:1.5.18")
    testImplementation("io.projectreactor:reactor-test:3.6.10")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("performance")
    }
    // Generate Jacoco report after every test run
    finalizedBy(tasks.jacocoTestReport)
}

// Configure Jacoco test report
tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required = true
        html.required = true
        csv.required = false
    }
}

// Configure Jacoco coverage verification
tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            limit {
                minimum = "0.80".toBigDecimal()
            }
        }
    }
}

// Run coverage verification as part of check
tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
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
    
    // Generate Jacoco report for performance tests too
    finalizedBy(tasks.jacocoTestReport)
}
