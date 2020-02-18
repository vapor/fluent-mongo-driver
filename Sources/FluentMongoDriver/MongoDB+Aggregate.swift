import FluentKit
import MongoKitten
import MongoCore

extension _MongoDB {
    func aggregate(
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
}
