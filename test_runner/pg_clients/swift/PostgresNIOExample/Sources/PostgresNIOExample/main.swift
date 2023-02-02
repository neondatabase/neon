import Foundation

import PostgresNIO
import NIOPosix
import Logging

await Task {
  do {
    let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    let logger = Logger(label: "postgres-logger")

    let env = ProcessInfo.processInfo.environment

    let sslContext = try! NIOSSLContext(configuration: .makeClientConfiguration())

    let config = PostgresConnection.Configuration(
      connection: .init(
        host: env["NEON_HOST"] ?? "",
        port: 5432
      ),
      authentication: .init(
        username: env["NEON_USER"] ?? "",
        database: env["NEON_DATABASE"] ?? "",
        password: env["NEON_PASSWORD"] ?? ""
      ),
      tls: .require(sslContext)
    )

    let connection = try await PostgresConnection.connect(
      on: eventLoopGroup.next(),
      configuration: config,
      id: 1,
      logger: logger
    )

    let rows = try await connection.query("SELECT 1 as col", logger: logger)
    for try await (n) in rows.decode((Int).self, context: .default) {
      print(n)
    }

    // Close your connection once done
    try await connection.close()

    // Shutdown the EventLoopGroup, once all connections are closed.
    try eventLoopGroup.syncShutdownGracefully()
  } catch {
      print(error)
  }
}.value
