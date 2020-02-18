import FluentKit
import MongoKitten
import MongoCore

struct _MongoDB: DatabaseDriver, MongoDB {
    func makeDatabase(with context: DatabaseContext) -> Database { self }
    
    func shutdown() { cluster.disconnect() }
    
    let cluster: MongoCluster
    public let raw: MongoDatabase
    let context: DatabaseContext
    
    init(cluster: MongoCluster, database: MongoDatabase) {
        self.cluster = cluster
        self.raw = database
        self.context = DatabaseContext(
            configuration: .init(),
            logger: Logger.defaultMongoCore,
            eventLoop: cluster.eventLoop
        )
    }
}

public protocol MongoDB {
    var raw: MongoDatabase { get }
}

extension _MongoDB: Database {
    func execute(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        switch query.action {
        case .create:
            return create(query: query, onRow: onRow)
        case .read:
            if
                query.fields.count == 1,
                case .aggregate(let aggregate) = query.fields[0],
                case .fields(let method, _) = aggregate
            {
                return self.aggregate(query: query, method: method, onRow: onRow)
            } else {
                return read(query: query, onRow: onRow)
            }
        case .update:
            return update(query: query, onRow: onRow)
        case .delete:
            return delete(query: query, onRow: onRow)
        case .custom:
            return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAction)
        }
    }
    
    func withConnection<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        closure(self)
    }
}

extension DatabaseDriverFactory {
    public static func mongo(
        settings: ConnectionSettings
    ) throws -> DatabaseDriverFactory {
        guard settings.hosts.count > 0 else {
            throw FluentMongoError.missingHosts
        }
        
        guard let targetDatabase = settings.targetDatabase else {
            throw FluentMongoError.noTargetDatabaseSpecified
        }
        
        return DatabaseDriverFactory { databases in
            do {
                let cluster = try MongoCluster(lazyConnectingTo: settings, on: databases.eventLoopGroup)
                return _MongoDB(
                    cluster: cluster,
                    database: cluster[targetDatabase]
                )
            } catch {
                fatalError("The MongoDB connection specification was malformed")
            }
        }
    }
    
    public static func mongo(
        connectionString: String
    ) throws -> DatabaseDriverFactory {
        return try .mongo(settings: ConnectionSettings(connectionString))
    }
}

