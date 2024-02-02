// swift-tools-version:5.8
import PackageDescription

let package = Package(
    name: "PostgresNIOExample",
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.16.0")
    ],
    targets: [
        .executableTarget(
            name: "PostgresNIOExample",
            dependencies: [
                .product(name: "PostgresNIO", package: "postgres-nio"),
            ]
	)
    ]
)
