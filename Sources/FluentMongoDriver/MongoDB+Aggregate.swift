import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func aggregate(
        query: DatabaseQuery,
        aggregate: DatabaseQuery.Aggregate,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        guard case .fields(let method, let field) = aggregate else {
            return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
        }
        
        switch method {
        case .count where query.joins.isEmpty:
            return count(query: query, onRow: onRow)
        case .count:
            return joinCount(query: query, onRow: onRow)
        case .sum:
            return group(
                query: query,
                mongoOperator: "$sum",
                field: field,
                onRow: onRow
            )
        case .average:
            return group(
                query: query,
                mongoOperator: "$avg",
                field: field,
                onRow: onRow
            )
        case .maximum:
            return group(
                query: query,
                mongoOperator: "$max",
                field: field,
                onRow: onRow
            )
        case .minimum:
            return group(
                query: query,
                mongoOperator: "$min",
                field: field,
                onRow: onRow
            )
        case .custom:
            return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
        }
    }
    
    private func count(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let condition = try query.makeMongoDBFilter()
            let count = CountCommand(on: query.schema, where: condition)
            
            return cluster.next(for: .init(writable: false)).flatMap { connection in
                return connection.executeCodable(
                    count,
                    namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                    sessionId: nil
                )
            }.decode(CountReply.self).flatMapThrowing { reply in
                let reply = _MongoDBAggregateResponse(value: reply.count, decoder: BSONDecoder())
                
                onRow(reply)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
    
    private func group(
        query: DatabaseQuery,
        mongoOperator: String,
        field: DatabaseQuery.Field,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let field = try field.makeMongoPath()
            let condition = try query.makeMongoDBFilter()
            let find = self.raw[query.schema]
                .find(condition)
                .project([
                    "n ": [
                        mongoOperator: "$\(field)"
                    ]
                ])
            
            return find.firstResult().map { result in
                onRow(_MongoDBAggregateResponse(value: result?["n"] ?? Null(), decoder: BSONDecoder()))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
