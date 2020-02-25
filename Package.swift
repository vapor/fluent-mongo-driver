// swift-tools-version:5.2
import PackageDescription

let package = Package(
    name: "fluent-mongo-driver",
    platforms: [
       .macOS(.v10_14)
    ],
    products: [
        .library(name: "FluentMongoDriver", targets: ["FluentMongoDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/fluent-kit.git", .branch("tn-beta-5")),
//        .package(url: "https://github.com/OpenKitten/MongoKitten.git", .branch("master/6.0")),
        .package(path: "/Users/joannisorlandos/Projects/MongoKitten"),
    ],
    targets: [
        .target(
            name: "FluentMongoDriver",
            dependencies: [
                .product(name: "FluentKit", package: "fluent-kit"),
                .product(name: "MongoKitten", package: "MongoKitten"),
            ]
        ),
        .testTarget(
            name: "FluentMongoDriverTests",
            dependencies: [
                .target(name: "FluentMongoDriver"),
                .product(name: "FluentBenchmark", package: "fluent-kit"),
            ]
        ),
    ]
)
