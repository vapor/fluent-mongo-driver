import FluentKit
import MongoKitten
import MongoCore

struct _MongoDB: DatabaseDriver, MongoDB {
    func makeDatabase(with context: DatabaseContext) -> Database { self }
    
    func shutdown() { cluster.disconnect() }
    
    let cluster: MongoCluster
    public let raw: MongoDatabase
    let context: DatabaseContext
    
    init(cluster: MongoCluster, databaseName: String) {
        self.cluster = cluster
        self.raw = cluster[databaseName]
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

struct _MongoDBEntity: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder
    
    var description: String { document.debugDescription }
    
    func contains(field: String) -> Bool {
        document.containsKey(field)
    }
    
    func decode<T>(field: String, as type: T.Type, for database: Database) throws -> T where T : Decodable {
        try decoder.decode(type, fromPrimitive: document[field] ?? Null())
    }
}

extension DatabaseQuery.Value {
    func makePrimitive() throws -> Primitive {
        switch self {
        case .array(let values):
            var array = Document(isArray: true)
            for value in values {
                try array.append(value.makePrimitive())
            }
            return array
        case .bind(let value):
            return try BSONEncoder().encodePrimitive(value) ?? Null()
        case .dictionary(let dict):
            var document = Document()
            
            for (key, value) in dict {
                document[key] = try value.makePrimitive()
            }
            
            return document
        case .null:
            return Null()
        case .default:
            throw FluentMongoError.unsupportedDefaultValue
        case .custom(let primitive as Primitive):
            return primitive
        case .custom:
            throw FluentMongoError.unsupportedCustomValue
        }
    }
}

extension DatabaseQuery.Filter.Method {
    var mongoOperator: String {
        switch self {
        case .equality(let inverse):
            return inverse ? "$ne" : "$eq"
        case .order(let inverse, let equality):
            switch (inverse, equality) {
            case (false, false):
                return "$gt"
            case (false, true):
                return "$gte"
            case (true, false):
                return "$lt"
            case (true, true):
                return "$lte"
            }
        case .subset:
            return "$in"
        case .custom, .contains:
            fatalError("Unsupported") // TODO:
        }
    }
}

extension DatabaseQuery.Sort.Direction {
    internal func mongo() throws -> SortOrder {
        switch self {
        case .ascending:
            return .ascending
        case .descending:
            return .descending
        case .custom(let order as SortOrder):
            return order
        case .custom:
            throw FluentMongoError.unsupportedCustomSort
        }
    }
}

extension DatabaseQuery {
    internal func makeMongoDBSort() throws -> MongoKitten.Sort? {
        var sortSpec = [(String, SortOrder)]()
        
        for sort in sorts {
            switch sort {
            case .sort(let field, let direction):
            switch field {
                case .field(let path, _, _):
                    let path = path.joined(separator: ".")
                    sortSpec.append((path, try direction.mongo()))
                case .custom, .aggregate:
                    throw FluentMongoError.unsupportedField
                }
            case .custom:
                throw FluentMongoError.unsupportedCustomSort
            }
        }
        
        if sortSpec.isEmpty {
            return nil
        }
        
        return MongoKitten.Sort(sortSpec)
    }
    
    internal func makeMongoDBFilter() throws -> Document {
        var conditions = [Document]()

        for filter in filters {
            switch filter {
            case .value(let field, let operation, let value):
                switch field {
                case .field(let path, _, _) where path.count == 1:
                    let filterOperator = operation.mongoOperator
                    var filter = Document()
                    filter[path[0]][filterOperator] = try value.makePrimitive()
                    conditions.append(filter)
                case .custom, .field, .aggregate:
                    throw FluentMongoError.unsupportedField
                }
            case .field:
                throw FluentMongoError.unsupportedFilter
            case .group:
                throw FluentMongoError.unsupportedFilter
            case .custom(let filter as Document):
                conditions.append(filter)
            case .custom:
                throw FluentMongoError.unsupportedCustomFilter
            }
        }
        
        if conditions.isEmpty {
            return [:]
        }
        
        if conditions.count == 1 {
            return conditions[0]
        }
        
        return AndQuery(conditions: conditions).makeDocument()
    }
    
    internal func makeValueDocuments() throws -> [Document] {
        let keys = try fields.map { field -> String in
            switch field {
            case .field(let path, _, _) where path.count == 1:
                return path[0]
            case .aggregate, .custom, .field:
                throw FluentMongoError.unsupportedField
            }
        }
        
        return try input.map { entity -> Document in
            assert(entity.count == keys.count, "The entity's keys.count != values.count")
            
            var document = Document()
            
            for index in 0..<keys.count {
                let key = keys[index]
                let value = try entity[index].makePrimitive()
                
                if key == "_id" {
                    document.insert(value, forKey: "_id", at: 0)
                } else {
                    document.appendValue(value, forKey: key)
                }
            }
            
            return document
        }
    }
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
    
    private func create(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let documents = try query.makeValueDocuments()
            
            return self.raw[query.schema]
                .insertMany(documents)
                .flatMapThrowing { reply in
                    guard reply.ok == 1 else {
                        throw FluentMongoError.insertFailed
                    }
                    
                    let reply = _MongoDBEntity(document: [
                        "insertCount": reply.insertCount
                    ], decoder: BSONDecoder())
                    onRow(reply)
                }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    private func aggregate(
        query: DatabaseQuery,
        method: DatabaseQuery.Field.Aggregate.Method,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        switch method {
        case .count:
            do {
                let condition = try query.makeMongoDBFilter()
                let count = CountCommand(on: query.schema, where: condition)
                
                return cluster.next(for: .init(writable: false)).flatMap { connection in
                    return connection.executeCodable(
                        count,
                        namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                        sessionId: nil
                    )
                }.flatMapThrowing { reply in
                    let reply = try BSONDecoder().decode(CountReply.self, from: reply.getDocument())
                    
                    onRow(
                        _MongoDBEntity(
                            document: ["fluentAggregate": reply.count],
                            decoder: BSONDecoder()
                        )
                    )
                }
            } catch {
                return eventLoop.makeFailedFuture(error)
            }
        case .average, .maximum, .minimum, .sum:
            // TODO:
            fallthrough
        case .custom:
            return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
        }
    }
    
    private func read(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let condition = try query.makeMongoDBFilter()
            let find = self.raw[query.schema].find(condition)
            
            switch query.limits.first {
            case .count(let limit):
                find.command.limit = limit
            case .custom:
                throw FluentMongoError.unsupportedCustomLimit
            case .none:
                break
            }
            
            switch query.offsets.first {
            case .count(let offset):
                find.command.skip = offset
            case .custom:
                throw FluentMongoError.unsupportedCustomLimit
            case .none:
                break
            }
            
            find.command.sort = try query.makeMongoDBSort()?.document
            
            if query.joins.count > 0 {
                fatalError()
            }
            
            return find.forEach { document in
                onRow(_MongoDBEntity(document: document, decoder: BSONDecoder()))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    private func update(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let filter = try query.makeMongoDBFilter()
            let update = try query.makeValueDocuments()
            
            let updates = update.map { document -> UpdateCommand.UpdateRequest in
                var update = UpdateCommand.UpdateRequest(
                    where: filter,
                    to: [
                        "$set": document
                    ]
                )
                
                update.multi = true
                
                return update
            }
            
            let command = UpdateCommand(updates: updates, inCollection: query.schema)
            return cluster.next(for: .init(writable: true)).flatMap { connection in
                return connection.executeCodable(
                    command,
                    namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                    sessionId: nil
                )
            }.flatMapThrowing { reply in
                onRow(
                    _MongoDBEntity(
                        document: try reply.getDocument(),
                        decoder: BSONDecoder()
                    )
                )
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    private func delete(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let filter = try query.makeMongoDBFilter()
            var deleteLimit: DeleteCommand.Limit = .all
            
            switch query.limits.first {
            case .count(let limit) where limit == 1:
                deleteLimit = .one
            case .custom, .count:
                throw FluentMongoError.unsupportedCustomLimit
            case .none:
                break
            }
            
            let command = DeleteCommand(
                where: filter,
                limit: deleteLimit,
                fromCollection: query.schema
            )
            
            return cluster.next(for: .init(writable: true)).flatMap { connection in
                return connection.executeCodable(
                    command,
                    namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                    sessionId: nil
                )
            }.flatMapThrowing { reply in
                onRow(
                    _MongoDBEntity(
                        document: try reply.getDocument(),
                        decoder: BSONDecoder()
                    )
                )
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    func execute(schema: DatabaseSchema) -> EventLoopFuture<Void> {
        raw.eventLoop.makeSucceededFuture(())
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
    case unsupportedField, unsupportedDefaultValue, insertFailed, unsupportedFilter
    case unsupportedCustomLimit, unsupportedCustomFilter, unsupportedCustomValue, unsupportedCustomAction, unsupportedCustomSort, unsupportedCustomAggregate
}
