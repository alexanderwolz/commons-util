import java.util.Base64

plugins {
    kotlin("jvm") version "2.2.10"
    id("java-library")
    id("io.github.gradle-nexus.publish-plugin") version "1.3.0"
    id("maven-publish")
    jacoco
    signing
}

group = "de.alexanderwolz"
version = "1.4.9"

repositories {
    mavenCentral()
    mavenLocal()
    maven("https://repo1.maven.org/maven2")
}

java {
    withSourcesJar()
    withJavadocJar()
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    api("de.alexanderwolz:commons-log:1.3.1")

    compileOnly("org.bouncycastle:bcpkix-jdk18on:1.81")
    compileOnly("jakarta.persistence:jakarta.persistence-api:3.1.0")

    testImplementation(kotlin("test"))
    testImplementation("org.slf4j:slf4j-simple:2.0.17")
    testImplementation("org.bouncycastle:bcpkix-jdk18on:1.81")
    testImplementation("jakarta.persistence:jakarta.persistence-api:3.1.0")
}

tasks.test {
    useJUnitPlatform()
    finalizedBy(tasks.jacocoTestReport)
}

jacoco {
    toolVersion = "0.8.11"
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "Alexander Wolz",
            "Built-By" to System.getProperty("user.name"),
            "Built-JDK" to System.getProperty("java.version"),
            "Created-By" to "Gradle ${gradle.gradleVersion}"
        )
    }
}

//see also https://github.com/gradle-nexus/publish-plugin/tree/v2.0.0
publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("Commons Util")
                description.set("Common utility classes for Java/Kotlin development")
                url.set("https://github.com/alexanderwolz/commons-util")
                licenses {
                    license {
                        name.set("Apache License 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("alexanderwolz")
                        name.set("Alexander Wolz")
                        url.set("https://www.alexanderwolz.de")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/alexanderwolz/commons-util.git")
                    developerConnection.set("scm:git:ssh://git@github.com/alexanderwolz/commons-util.git")
                    url.set("https://github.com/alexanderwolz/commons-util")
                }
            }
        }
    }
}

signing {
    val signingKey = System.getenv("GPG_PRIVATE_KEY")
    val signingPassword = System.getenv("GPG_PASSPHRASE")

    if (signingKey != null && signingPassword != null) {
        logger.info("GPG credentials found in System")
        val decodedKey = String(Base64.getDecoder().decode(signingKey))
        useInMemoryPgpKeys(decodedKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    } else {
        logger.info("No GPG credentials found in System, using cmd..")
        useGpgCmd()
        sign(publishing.publications["mavenJava"])
    }
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
            username.set(System.getenv("SONATYPE_USERNAME"))
            password.set(System.getenv("SONATYPE_PASSWORD"))
        }
    }
}
