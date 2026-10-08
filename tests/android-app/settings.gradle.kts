// The test-only Android app of the `android-app` and `android-app-release` end-to-end targets: it
// runs the shared `tests/e2e` scenarios inside an app process through the `tests/android-harness`
// JNI library. Built and run only by the Aspire runner (`dotnet/RayX.SignalR.E2E.Tests`).
pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
        // The Kotlin component of rustls-platform-verifier (its README's Gradle setup).
        maven {
            url = uri("https://github.com/rustls/rustls-platform-verifier/raw/maven-archive/android-release-support/maven/")
        }
    }
}

rootProject.name = "rayx-signalr-android-app"
include(":app")
