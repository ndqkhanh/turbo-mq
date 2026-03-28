dependencies {
    implementation(project(":common"))
    implementation(project(":raft"))
    implementation(project(":storage"))
    implementation(project(":network"))
    implementation(rootProject.libs.micrometer.core)
    implementation(rootProject.libs.micrometer.prometheus)
}
