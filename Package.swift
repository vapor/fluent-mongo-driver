// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "fluent-mongo-driver",
    platforms: [
       .macOS(.v10_14)
    ],
    products: [
        .library(
            name: "FluentMongoDriver",
            targets: ["FluentMongoDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/fluent-kit.git", .branch("tn-field-key")),
        .package(url: "https://github.com/OpenKitten/MongoKitten.git", from: "6.0.0"),
    ],
    targets: [
        .target(
            name: "FluentMongoDriver",
            dependencies: ["FluentKit", "MongoKitten"]),
        .testTarget(
            name: "FluentMongoDriverTests",
            dependencies: ["FluentMongoDriver", "FluentBenchmark"]),
    ]
)
