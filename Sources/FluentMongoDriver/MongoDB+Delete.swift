import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func delete(
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
            }.decode(DeleteReply.self).flatMapThrowing { reply in
                let reply = _MongoDBAggregateResponse(
                    value: reply.deletes,
                    decoder: BSONDecoder()
                )
                
                onRow(reply)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
