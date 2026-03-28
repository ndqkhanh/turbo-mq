dependencies {
    implementation(project(":common"))
    implementation(project(":raft"))

    // Test framework is a library used by other modules' tests,
    // so its main sources need test dependencies
    implementation(platform(rootProject.libs.junit.bom))
    implementation(rootProject.libs.junit.jupiter)
    implementation(rootProject.libs.assertj.core)
}
