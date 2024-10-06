import FluentKit
@preconcurrency import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    @preconcurrency
    func delete(
        query: DatabaseQuery,
        onOutput: @Sendable @escaping (any DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let filter = try query.makeMongoDBFilter(aggregate: false)
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
            
            logger.debug("fluent-mongo delete \(deleteLimit) filter=\(filter)")
            return cluster.next(for: .init(writable: true)).flatMap { connection in
                return connection.executeCodable(
                    command,
                    namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                    sessionId: nil
                )
            }.decodeReply(DeleteReply.self).hop(to: eventLoop).flatMapThrowing { reply in
                let reply = _MongoDBAggregateResponse(
                    value: reply.deletes,
                    decoder: BSONDecoder()
                )
                onOutput(reply)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
