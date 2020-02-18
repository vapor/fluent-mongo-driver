import FluentKit
import MongoKitten
import MongoCore

extension _MongoDB {
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
                    }.map { _ in }
                    
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
}
