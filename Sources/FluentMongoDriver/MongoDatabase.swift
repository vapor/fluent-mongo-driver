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

extension FieldKey {
    func makeMongoKey() throws -> String {
        switch self {
        case .id:
            return "_id"
        case .name(let name):
            return name
        case .prefixed:
            throw FluentMongoError.unsupportedJoin
        }
    }
}

struct _MongoDBEntity: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder
    
    var description: String { document.debugDescription }
    
    func contains(field: FieldKey) -> Bool {
        do {
            return try document.containsKey(field.makeMongoKey())
        } catch {
            return false
        }
    }
    
    func decode<T>(field: FieldKey, as type: T.Type, for database: Database) throws -> T where T : Decodable {
        try decoder.decode(type, fromPrimitive: document[field.makeMongoKey()] ?? Null())
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
    func makeMongoOperator() throws -> String {
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
            throw FluentMongoError.unsupportedOperator
        }
    }
}

extension DatabaseQuery.Sort.Direction {
    internal func makeMongoDirection() throws -> SortOrder {
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

extension DatabaseQuery.Filter {
    internal func makeMongoDBFilter() throws -> Document {
        switch self {
        case .value(let field, let operation, let value):
            switch field {
            case .field(let path, _, _):
                let filterOperator = try operation.makeMongoOperator()
                var filter = Document()
                let path = try path.map { try $0.makeMongoKey() }.joined(separator: ".")
                try filter[path][filterOperator] = value.makePrimitive()
                return filter
            case .custom, .aggregate:
                throw FluentMongoError.unsupportedField
            }
        case .field:
            throw FluentMongoError.unsupportedFilter
        case .group(let conditions, let relation):
            let conditions = try conditions.map { condition in
                return try condition.makeMongoDBFilter()
            }
            
            switch relation {
            case .and:
                return AndQuery(conditions: conditions).makeDocument()
            case .or:
                return OrQuery(conditions: conditions).makeDocument()
            case .custom:
                throw FluentMongoError.unsupportedCustomFilter
            }
        case .custom(let filter as Document):
            return filter
        case .custom:
            throw FluentMongoError.unsupportedCustomFilter
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
                    let path = try path.map { try $0.makeMongoKey() }.joined(separator: ".")
                    try sortSpec.append((path, direction.makeMongoDirection()))
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
            conditions.append(try filter.makeMongoDBFilter())
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
                return try path[0].makeMongoKey()
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
                    print("create", reply)
                    guard reply.ok == 1, reply.insertCount == documents.count else {
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
            if query.joins.count > 0 {
                throw FluentMongoError.unsupportedJoin
            }
            
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
        do {
            var futures = [EventLoopFuture<Void>]()
            
            nextConstraint: for constraint in schema.constraints {
                switch constraint {
                case .unique(let fields):
                    let indexKeys = try fields.map { field -> String in
                        switch field {
                        case .key(let key):
                            return try key.makeMongoKey()
                        case .custom:
                            throw FluentMongoError.invalidIndexKey
                        }
                    }
                    
                    var keys = Document()
                    
                    for key in indexKeys {
                        keys[key] = SortOrder.ascending.rawValue
                    }
                    
                    var index = CreateIndexes.Index(
                        named: "unique",
                        keys: keys
                    )
                    
                    index.unique = true
                    
                    let createIndexes = CreateIndexes(
                        collection: schema.schema,
                        indexes: [index]
                    )
                    
                    let createdIndex = cluster.next(for: .init(writable: false)).flatMap { connection in
                        return connection.executeCodable(
                            createIndexes,
                            namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                            sessionId: nil
                        )
                    }.map { reply in
                        print(reply)
                    }
                    
                    futures.append(createdIndex)
                case .foreignKey, .custom:
                    continue nextConstraint
                }
            }
            
            return EventLoopFuture.andAllSucceed(futures, on: eventLoop)
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        do {
            let transactionDatabase = try raw.startTransaction(autoCommitChanges: false)
            let database = _MongoDB(
                cluster: self.cluster,
                database: transactionDatabase
            )
            return closure(database).flatMap { value in
                transactionDatabase.commit().map { value }
            }.flatMapError { error in
                transactionDatabase.abort().flatMapThrowing { _ in
                    throw error
                }
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
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

extension MongoWriteError: DatabaseError {
    public var isSyntaxError: Bool { false }
    public var isConstraintFailure: Bool { false }
    public var isConnectionClosed: Bool { false }
}

extension MongoError: DatabaseError {
    public var isSyntaxError: Bool { false }
    public var isConstraintFailure: Bool { false }
    public var isConnectionClosed: Bool { false }
}

enum FluentMongoError: Error, DatabaseError {
    var isSyntaxError: Bool { false }
    var isConstraintFailure: Bool { false }
    var isConnectionClosed: Bool { false }
    
    case missingHosts, noTargetDatabaseSpecified, unsupportedJoin, unsupportedOperator, invalidIndexKey
    case unsupportedField, unsupportedDefaultValue, insertFailed, unsupportedFilter
    case unsupportedCustomLimit, unsupportedCustomFilter, unsupportedCustomValue, unsupportedCustomAction, unsupportedCustomSort, unsupportedCustomAggregate
}
