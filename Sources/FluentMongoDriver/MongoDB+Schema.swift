import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func execute(schema: DatabaseSchema) -> EventLoopFuture<Void> {
        switch schema.action {
        case .create, .update:
            return self.update(schema: schema)
        case .delete:
            return self.delete(schema: schema)
        }
    }

    private func update(schema: DatabaseSchema) -> EventLoopFuture<Void> {
        do {
            var futures = [EventLoopFuture<Void>]()

            nextConstraint: for constraint in schema.createConstraints {
                guard case .constraint(let algorithm, let name) = constraint else {
                    continue nextConstraint
                }
                switch algorithm {
                case .unique(let fields), .compositeIdentifier(let fields):
                    let indexKeys = try fields.map { field -> String in
                        switch field {
                        case .key(let key):
                            return key.makeMongoKey()
                        case .custom:
                            throw FluentMongoError.invalidIndexKey
                        }
                    }

                    var keys = Document()

                    for key in indexKeys {
                        keys[key] = SortOrder.ascending.rawValue
                    }

                    var index = CreateIndexes.Index(
                        named: name ?? "unique",
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
                    }.hop(to: eventLoop).map { _ in }

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

    private func delete(schema: DatabaseSchema) -> EventLoopFuture<Void> {
        self.raw[schema.schema].drop()
    }
}
