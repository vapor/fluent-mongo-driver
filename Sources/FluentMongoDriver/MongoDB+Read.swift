import FluentKit
import MongoKitten
import MongoCore

extension _MongoDB {
    func read(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            if query.joins.count > 0 {
                throw FluentMongoError.unsupportedJoin
            }
            
            let condition = try query.makeMongoDBFilter()
            let find = self.raw[query.schema].find(condition)
            
            switch query.limits.first {
            case .count(let limit):
                find.command.limit = limit
            case .custom:
                throw FluentMongoError.unsupportedCustomLimit
            case .none:
                break
            }
            
            switch query.offsets.first {
            case .count(let offset):
                find.command.skip = offset
            case .custom:
                throw FluentMongoError.unsupportedCustomLimit
            case .none:
                break
            }
            
            find.command.sort = try query.makeMongoDBSort()?.document
            
            return find.forEach { document in
                onRow(_MongoDBEntity(document: document, decoder: BSONDecoder()))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
