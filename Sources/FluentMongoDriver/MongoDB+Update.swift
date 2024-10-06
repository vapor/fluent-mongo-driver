import FluentKit
@preconcurrency import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    @preconcurrency
    func update(
        query: DatabaseQuery,
        onOutput: @Sendable @escaping (any DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let filter = try query.makeMongoDBFilter(aggregate: false)
            let update = try query.makeValueDocuments()
            
            let updates = update.map { document -> UpdateCommand.UpdateRequest in
                var update = UpdateCommand.UpdateRequest(
                    where: filter,
                    to: [
                        "$set": document
                    ]
                )
                
                update.multi = true
                
                return update
            }
            
            let command = UpdateCommand(updates: updates, inCollection: query.schema)
            logger.debug("fluent-mongo update filter=\(filter) updates=\(update)")
            return cluster.next(for: .init(writable: true)).flatMap { connection in
                return connection.executeCodable(
                    command,
                    namespace: MongoNamespace(to: "$cmd", inDatabase: self.raw.name),
                    sessionId: nil
                )
            }.decodeReply(UpdateReply.self).hop(to: eventLoop).flatMapThrowing { reply in
                let reply = _MongoDBAggregateResponse(
                    value: reply.updatedCount,
                    decoder: BSONDecoder()
                )
                onOutput(reply)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
