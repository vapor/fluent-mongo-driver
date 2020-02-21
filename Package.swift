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
        .package(url: "https://github.com/vapor/fluent-kit.git", .revision("tn-beta-5")),
        .package(url: "https://github.com/OpenKitten/MongoKitten.git", from: "6.0.0"),
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
