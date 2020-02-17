// swift-tools-version:5.1
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "FluentMongo",
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "FluentMongo",
            targets: ["FluentMongo"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        .package(url: "https://github.com/vapor/fluent-kit.git", from: "1.0.0-beta.2"),
        .package(url: "https://github.com/OpenKitten/MongoKitten.git", from: "6.0.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "FluentMongo",
            dependencies: ["FluentKit", "MongoKitten"]),
        .testTarget(
            name: "FluentMongoTests",
            dependencies: ["FluentMongo"]),
    ]
)
