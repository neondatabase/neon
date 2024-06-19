// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "PostgresNIOExample",
    dependencies: [
        .package(url: "https://github.com/vapor/postgres-nio.git", from: "1.20.2")
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
