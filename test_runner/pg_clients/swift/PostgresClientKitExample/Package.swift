// swift-tools-version:5.6
import PackageDescription

let package = Package(
    name: "PostgresClientKitExample",
    dependencies: [
        .package(
            url: "https://github.com/codewinsdotcom/PostgresClientKit.git",
            revision: "v1.4.3"
        )
    ],
    targets: [
        .target(
            name: "PostgresClientKitExample",
            dependencies: [ "PostgresClientKit" ])
    ]
)
