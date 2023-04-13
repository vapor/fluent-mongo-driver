// swift-tools-version:5.6
import PackageDescription

let package = Package(
    name: "fluent-mongo-driver",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
        .watchOS(.v6),
        .tvOS(.v13),
    ],
    products: [
        .library(name: "FluentMongoDriver", targets: ["FluentMongoDriver"]),
    ],
    dependencies: [
        .package(url: "https://github.com/vapor/fluent-kit.git", from: "1.37.0"),
        .package(url: "https://github.com/OpenKitten/MongoKitten.git", from: "6.6.4"),
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
