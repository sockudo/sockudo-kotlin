plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    `java-library`
    `maven-publish`
    signing
}

group = "io.sockudo"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    api(libs.coroutines.core)
    api(libs.kotlinx.serialization.json)
    api(libs.okhttp)
    implementation(libs.tweetnacl)
    implementation(libs.vcdiff)
    runtimeOnly(libs.slf4j.nop)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.coroutines.test)
    testImplementation(libs.junit.jupiter)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }

    withJavadocJar()
    withSourcesJar()
}

kotlin {
    compilerOptions {
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifactId = "sockudo-kotlin"

            pom {
                name.set("sockudo-kotlin")
                description.set("Sockudo Kotlin client port.")
                url.set("https://github.com/sockudo/sockudo-kotlin")
                licenses {
                    license {
                        name.set("MIT")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
                developers {
                    developer {
                        id.set("sockudo")
                        name.set("Sockudo")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/sockudo/sockudo-kotlin.git")
                    developerConnection.set("scm:git:ssh://git@github.com/sockudo/sockudo-kotlin.git")
                    url.set("https://github.com/sockudo/sockudo-kotlin")
                }
            }
        }
    }
}
