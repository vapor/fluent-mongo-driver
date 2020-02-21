import FluentKit
import MongoKitten
import MongoCore

extension DatabaseID {
    public static var mongo: DatabaseID {
        return .init(string: "mongo")
    }
}

struct FluentMongoDatabase: Database, MongoDatabaseRepresentable {
    let cluster: MongoCluster
    let raw: MongoDatabase
    let context: DatabaseContext

    func execute(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        switch query.action {
        case .create:
            return self.create(query: query, onRow: onRow)
        case .aggregate(let aggregate):
            return self.aggregate(query: query, aggregate: aggregate, onRow: onRow)
        case .read where query.joins.isEmpty:
            return self.read(query: query, onRow: onRow)
        case .read:
            return self.join(query: query, onRow: onRow)
        case .update:
            return self.update(query: query, onRow: onRow)
        case .delete:
            return self.delete(query: query, onRow: onRow)
        case .custom:
            return self.eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAction)
        }
    }

    func execute(enum: DatabaseEnum) -> EventLoopFuture<Void> {
        self.raw.eventLoop.makeSucceededFuture(())
    }

    func withConnection<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

struct FluentMongoDriver: DatabaseDriver {
    func makeDatabase(with context: DatabaseContext) -> Database {
        FluentMongoDatabase(
            cluster: self.cluster,
            raw: self.cluster[self.targetDatabase],
            context: context
        )
    }
    
    let cluster: MongoCluster
    let targetDatabase: String

    func shutdown() {
        self.cluster.disconnect()
    }
}

public protocol MongoDatabaseRepresentable {
    var raw: MongoDatabase { get }
}

struct FluentMongoConfiguration: DatabaseConfiguration {
    let settings: ConnectionSettings
    let targetDatabase: String
    var middleware: [AnyModelMiddleware]

    func makeDriver(for databases: Databases) -> DatabaseDriver {
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

