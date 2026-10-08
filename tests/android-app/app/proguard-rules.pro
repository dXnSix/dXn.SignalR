# The keep rule rustls-platform-verifier documents (and docs/09-platforms.md gives consumers): its
# Kotlin classes are called from Rust through JNI, so R8 must not remove or rename them.
-keep, includedescriptorclasses class org.rustls.platformverifier.** { *; }
