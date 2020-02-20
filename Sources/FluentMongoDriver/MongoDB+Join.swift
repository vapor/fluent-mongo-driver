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
            
            return self.raw[query.schema].aggregate(stages).forEach { document in
                print(document)
                onRow(_MongoDBEntity(
                    document: document,
                    decoder: BSONDecoder(),
                    aggregateQuery: query
                ))
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
        
        var moveProjection = Document()
        
        for field in fields {
            try moveProjection[field.makeProjectedMongoPath()] = "$\(field.makeMongoPath())"
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
