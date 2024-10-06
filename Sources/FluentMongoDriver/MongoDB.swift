import FluentKit
@preconcurrency import MongoKitten
@preconcurrency import MongoCore

extension DatabaseID {
    public static var mongo: DatabaseID {
        return .init(string: "mongo")
    }
}

struct FluentMongoDatabase: Database, MongoDatabaseRepresentable {
    let cluster: MongoCluster
    let raw: MongoDatabase
    let context: DatabaseContext
    let inTransaction: Bool

    @preconcurrency
    func execute(
        query: DatabaseQuery,
        onOutput: @Sendable @escaping (any DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        switch query.action {
        case .create:
            return self.create(query: query, onOutput: onOutput)
        case .aggregate(let aggregate):
            return self.aggregate(query: query, aggregate: aggregate, onOutput: onOutput)
        case .read where query.joins.isEmpty:
            return self.read(query: query, onOutput: onOutput)
        case .read:
            return self.join(query: query, onOutput: onOutput)
        case .update:
            return self.update(query: query, onOutput: onOutput)
        case .delete:
            return self.delete(query: query, onOutput: onOutput)
        case .custom:
            return self.eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAction)
        }
    }

    func execute(enum: DatabaseEnum) -> EventLoopFuture<Void> {
        self.raw.eventLoop.makeSucceededFuture(())
    }

    func withConnection<T>(_ closure: @escaping (any Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

struct FluentMongoDriver: DatabaseDriver {
    func makeDatabase(with context: DatabaseContext) -> any Database {
        FluentMongoDatabase(
            cluster: self.cluster,
            raw: self.cluster[self.targetDatabase].hopped(to: context.eventLoop),
            context: context,
            inTransaction: false
        )
    }
    
    let cluster: MongoCluster
    let targetDatabase: String

    func shutdown() {
        try? self.cluster.disconnect().wait()
    }
    
    func shutdownAsync() async {
        try? await self.cluster.disconnect().get()
    }
}

public protocol MongoDatabaseRepresentable {
    var raw: MongoDatabase { get }
}

struct FluentMongoConfiguration: DatabaseConfiguration {
    let settings: ConnectionSettings
    let targetDatabase: String
    var middleware: [any AnyModelMiddleware]

    func makeDriver(for databases: Databases) -> any DatabaseDriver {
        do {
            let cluster = try MongoCluster(lazyConnectingTo: self.settings, on: databases.eventLoopGroup)
            return FluentMongoDriver(
                cluster: cluster,
                targetDatabase: self.targetDatabase
            )
        } catch {
            fatalError("The MongoDB connection specification was malformed")
        }
    }
}

extension DatabaseConfigurationFactory {
    public static func mongo(
        connectionString: String
    ) throws -> Self {
        return try .mongo(settings: ConnectionSettings(connectionString))
    }

    public static func mongo(
        settings: ConnectionSettings
    ) throws -> Self {
        guard settings.hosts.count > 0 else {
            throw FluentMongoError.missingHosts
        }

        guard let targetDatabase = settings.targetDatabase else {
            throw FluentMongoError.noTargetDatabaseSpecified
        }

        return .init {
            FluentMongoConfiguration(
                settings: settings,
                targetDatabase:
                targetDatabase, middleware: []
            )
        }
    }
}

