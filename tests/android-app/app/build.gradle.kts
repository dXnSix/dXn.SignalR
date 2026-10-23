plugins {
    id("com.android.application")
}

// The version of rustls-platform-verifier's Kotlin component must be the version of the
// `rustls-platform-verifier-android` crate in the Cargo lockfile (its README's Gradle setup).
abstract class RustlsVersion : ValueSource<String, RustlsVersion.Params> {
    interface Params : ValueSourceParameters {
        val lockFile: RegularFileProperty
    }

    companion object {
        const val CRATE_NAME = "rustls-platform-verifier-android"
    }

    override fun obtain(): String {
        val lines = parameters.lockFile.get().asFile.readLines()
        val nameIndex = lines.indexOfFirst { it.trim() == "name = \"$CRATE_NAME\"" }
        return lines.drop(nameIndex + 1)
            .firstOrNull { nameIndex >= 0 && it.trimStart().startsWith("version = ") }
            ?.substringAfter('"', "")
            ?.substringBefore('"', "")
            ?.takeIf { it.isNotEmpty() }
            ?: error("$CRATE_NAME not found in the Cargo lockfile")
    }
}

val rustlsPlatformVerifierVersion = providers.of(RustlsVersion::class.java) {
    parameters.lockFile.set(rootProject.layout.projectDirectory.file("../android-harness/Cargo.lock"))
}

// The harness library the runner builds with `cargo ndk` (`AndroidAppCellRunner`).
val jniLibsDirectory = providers.gradleProperty("dxn.jniLibs").orElse("build/dxn-jniLibs")

android {
    namespace = "dev.dxn.signalr.e2e"
    compileSdk = 36

    defaultConfig {
        applicationId = "dev.dxn.signalr.e2e"
        minSdk = 24
        targetSdk = 35
        versionCode = 1
        versionName = "1.0"
    }

    sourceSets {
        getByName("main") {
            jniLibs.directories.clear()
            jniLibs.directories.add(jniLibsDirectory.get())
        }
    }

    buildTypes {
        getByName("release") {
            // R8 with the keep rule the documentation gives consumers (`proguard-rules.pro`).
            isMinifyEnabled = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            // Signed with the generated debug key so the runner can install it.
            signingConfig = signingConfigs.getByName("debug")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    // rustls-platform-verifier is a Rust crate with a Kotlin component.
    implementation("org.rustls:rustls-platform-verifier:${rustlsPlatformVerifierVersion.get()}")
}
