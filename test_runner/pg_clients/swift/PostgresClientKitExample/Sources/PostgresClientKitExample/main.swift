import Foundation

import PostgresClientKit

do {
    var configuration = PostgresClientKit.ConnectionConfiguration()

    let env = ProcessInfo.processInfo.environment
    if let host = env["NEON_HOST"] {
        configuration.host = host
    }
    if let database = env["NEON_DATABASE"] {
        configuration.database = database
    }
    if let user = env["NEON_USER"] {
        configuration.user = user
    }
    if let password = env["NEON_PASSWORD"] {
        configuration.credential = .scramSHA256(password: password)
    }

    let connection = try PostgresClientKit.Connection(configuration: configuration)
    defer { connection.close() }

    let text = "SELECT 1;"
    let statement = try connection.prepareStatement(text: text)
    defer { statement.close() }

    let cursor = try statement.execute(parameterValues: [ ])
    defer { cursor.close() }

    for row in cursor {
        let columns = try row.get().columns
        print(columns[0])
    }
} catch {
    print(error)
}
