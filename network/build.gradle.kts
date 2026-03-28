plugins {
    id("com.google.protobuf") version "0.9.4"
}

dependencies {
    implementation(project(":common"))
    implementation(project(":raft"))
    implementation(project(":storage"))

    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.services)
    implementation(libs.protobuf.java)
    implementation(libs.javax.annotation.api)

    testImplementation(libs.grpc.testing)
    testImplementation("io.grpc:grpc-inprocess:1.62.2")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.62.2"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc")
            }
        }
    }
}
