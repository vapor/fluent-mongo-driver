import FluentKit
import MongoKitten
import MongoCore

struct _MongoDB: DatabaseDriver {
    func makeDatabase(with context: DatabaseContext) -> Database { self }
    
    func shutdown() { cluster.disconnect() }
    
    let cluster: MongoCluster
    let database: MongoDatabase
    let context: DatabaseContext
    
    init(cluster: MongoCluster, databaseName: String) {
        self.cluster = cluster
        self.database = cluster[databaseName]
        self.context = DatabaseContext(
            configuration: .init(),
            logger: Logger.defaultMongoCore,
            eventLoop: cluster.eventLoop
        )
    }
}

struct _MongoDBEntity: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder
    
    var description: String { document.debugDescription }
    
    func contains(field: String) -> Bool {
        // TODO: MongoKitten should add a faster key-by-key iterator or implement `.containsKey` on Document
        document.keys.contains(field)
    }
    
    func decode<T>(field: String, as type: T.Type, for database: Database) throws -> T where T : Decodable {
        try decoder.decode(type, fromPrimitive: document[field] ?? Null())
    }
}

extension _MongoDB: Database {
    func execute(query: DatabaseQuery, onRow: @escaping (DatabaseRow) -> ()) -> EventLoopFuture<Void> {
        fatalError("Unimplemented")
    }

    func execute(schema: DatabaseSchema) -> EventLoopFuture<Void> {
        database.eventLoop.makeSucceededFuture(())
    }

    func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
//        do {
//            let transactionDatabase = try database.startTransaction(autoCommitChanges: false)
//            let database = _MongoDB(database: transactionDatabase)
//            return closure(database)
//             TODO: Commit
//        } catch {
//            return database.eventLoop.makeFailedFuture(error)
//        }
        
        fatalError("Unimplemented")
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
                    databaseName: targetDatabase
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

enum FluentMongoError: Error {
    case missingHosts, noTargetDatabaseSpecified
}
