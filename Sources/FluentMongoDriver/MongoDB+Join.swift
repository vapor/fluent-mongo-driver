import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func join(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let stages = try query.makeAggregatePipeline()
            let decoder = BSONDecoder()
            var aliases: [String: (schema: String, key: String)] = [:]
            for field in query.fields {
                switch field {
                case .field(let path, let schema, let alias):
                    if let alias = alias {
                        aliases[alias] = (schema!, path[0].makeMongoKey())
                    }
                default:
                    fatalError("Unsupported field: \(field).")
                }
            }
            return self.raw[query.schema].aggregate(stages).forEach { document in
                let row = FluentMongoJoinedRow(
                    document: document,
                    decoder: decoder,
                    schema: query.schema,
                    aliases: aliases
                )
                onRow(row)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    func joinCount(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let stages = try query.makeAggregatePipeline()
            return self.raw[query.schema].aggregate(stages).count().map { count in
                let reply = _MongoDBAggregateResponse(value: count, decoder: BSONDecoder())
                
                onRow(reply)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}

extension DatabaseQuery {
    func makeAggregatePipeline() throws -> [AggregateBuilderStage] {
        var stages = [AggregateBuilderStage]()
        
        let filter = try makeMongoDBFilter()
        
        if !filter.isEmpty {
            stages.append(match(filter))
        }
        
        switch limits.first {
        case .count(let n):
            stages.append(limit(n))
        case .custom:
            throw FluentMongoError.unsupportedCustomLimit
        case .none:
            break
        }
        
        switch offsets.first {
        case .count(let offset):
            stages.append(skip(offset))
        case .custom:
            throw FluentMongoError.unsupportedCustomLimit
        case .none:
            break
        }
        
        stages.append(AggregateBuilderStage(document: [
            "$replaceRoot": [
                "newRoot": [
                    self.schema: "$$ROOT"
                ]
            ]
        ]))
        
        for join in joins {
            switch join {
            case .join(let foreignCollection, let foreignKey, let localKey, let method):
                guard case .schema(let collection, let alias) = foreignCollection else {
                    throw FluentMongoError.unsupportedJoin
                }
                
                switch method {
                case .left, .outer:
                    stages.append(lookup(
                        from: collection,
                        localField: try localKey.makeProjectedMongoPath(),
                        foreignField: try foreignKey.makeMongoPath(),
                        as: alias ?? collection
                    ))
                case .inner:
                    stages.append(lookup(
                        from: collection,
                        localField: try localKey.makeProjectedMongoPath(),
                        foreignField: try foreignKey.makeMongoPath(),
                        as: alias ?? collection
                    ))

                    stages.append(AggregateBuilderStage(document: [
                        "$unwind": "$\(alias ?? collection)"
                    ]))
                case .right, .custom:
                    fatalError()
                }
            case .custom:
                throw FluentMongoError.unsupportedJoin
            }
        }
        
        return stages
    }
}

private struct FluentMongoJoinedRow: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder
    let schema: String
    let aliases: [String: (schema: String, key: String)]

    var description: String {
        self.document.debugDescription
    }

    func contains(field: FieldKey) -> Bool {
        self.primitive(field: field) != nil
    }

    func decode<T>(field: FieldKey, as type: T.Type, for database: Database) throws -> T
        where T : Decodable
    {
        try self.decoder.decode(
            type,
            fromPrimitive: self.primitive(field: field) ?? Null()
        )
    }

    private func primitive(field: FieldKey) -> Primitive? {
        let key = field.makeMongoKey()
        if let (schema, field) = self.aliases[key] {
            return self.document[schema][field]
        } else {
            return self.document[self.schema][key]
        }
    }
}
