dependencies {
    implementation(project(":common"))
    implementation(project(":raft"))
    implementation(rootProject.libs.rocksdb)
}
