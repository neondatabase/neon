// swift-tools-version:5.10
import PackageDescription

let package = Package(
    name: "PostgresClientKitExample",
    dependencies: [
        .package(
            url: "https://github.com/codewinsdotcom/PostgresClientKit.git",
            revision: "v1.5.0"
        )
    ],
    targets: [
        .executableTarget(
            name: "PostgresClientKitExample",
            dependencies: [ "PostgresClientKit" ])
    ]
)
