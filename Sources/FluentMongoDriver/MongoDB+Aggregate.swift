import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func aggregate(
        query: DatabaseQuery,
        aggregate: DatabaseQuery.Aggregate,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        guard case .field(let field, let method) = aggregate else {
            return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
        }
        
        switch method {
        case .count where query.joins.isEmpty:
            return count(query: query, onOutput: onOutput)
        case .count:
            return joinCount(query: query, onOutput: onOutput)
        case .sum:
            return group(
                query: query,
                mongoOperator: "$sum",
                field: field,
                onOutput: onOutput
            )
        case .average:
            return group(
                query: query,
                mongoOperator: "$avg",
                field: field,
                onOutput: onOutput
            )
        case .maximum:
            return group(
                query: query,
                mongoOperator: "$max",
                field: field,
                onOutput: onOutput
            )
        case .minimum:
            return group(
                query: query,
                mongoOperator: "$min",
                field: field,
                onOutput: onOutput
            )
        case .custom:
            return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
        }
    }
    
    private func count(
        query: DatabaseQuery,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let condition = try query.makeMongoDBFilter(aggregate: false)
            let count = CountCommand(on: query.schema, where: condition)
            
            return cluster.next(for: .init(writable: false)).flatMap { connection in
                return connection.executeCodable(
                    count,
                    namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                    sessionId: nil
                )
            }.decode(CountReply.self).hop(to: eventLoop).flatMapThrowing { reply in
                let reply = _MongoDBAggregateResponse(value: reply.count, decoder: BSONDecoder())
                onOutput(reply)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    private func group(
        query: DatabaseQuery,
        mongoOperator: String,
        field: DatabaseQuery.Field,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let field = try field.makeMongoPath()
            let condition = try query.makeMongoDBFilter(aggregate: false)
            let find = self.raw[query.schema]
                .find(condition)
                .project([
                    "n": [
                        mongoOperator: "$\(field)"
                    ]
                ])
            return find.firstResult().map { result in
                let res = _MongoDBAggregateResponse(
                    value: result?["n"] ?? Null(),
                    decoder: BSONDecoder()
                )
                onOutput(res)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
