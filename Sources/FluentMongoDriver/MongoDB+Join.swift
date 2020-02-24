import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func join(
        query: DatabaseQuery,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let stages = try query.makeAggregatePipeline()
            let decoder = BSONDecoder()
            return self.raw[query.schema].aggregate(stages).forEach { document in
                onOutput(document.databaseOutput(using: decoder))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    func joinCount(
        query: DatabaseQuery,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let stages = try query.makeAggregatePipeline()
            return self.raw[query.schema].aggregate(stages).count().map { count in
                let reply = _MongoDBAggregateResponse(value: count, decoder: BSONDecoder())
                onOutput(reply)
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
            case .join(let schema, let alias, let method, let foreignKey, let localKey):
                switch method {
                case .left:
                    stages.append(lookup(
                        from: schema,
                        localField: try localKey.makeProjectedMongoPath(),
                        foreignField: try foreignKey.makeMongoPath(),
                        as: alias ?? schema
                    ))
                case .inner:
                    stages.append(lookup(
                        from: schema,
                        localField: try localKey.makeProjectedMongoPath(),
                        foreignField: try foreignKey.makeMongoPath(),
                        as: alias ?? schema
                    ))

                    stages.append(AggregateBuilderStage(document: [
                        "$unwind": "$\(alias ?? schema)"
                    ]))
                case .custom:
                    fatalError()
                }
            case .custom:
                throw FluentMongoError.unsupportedJoin
            }
        }
        
        return stages
    }
}
