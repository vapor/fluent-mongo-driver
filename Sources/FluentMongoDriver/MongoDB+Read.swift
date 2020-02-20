import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func read(
        query: DatabaseQuery,
        onRow: @escaping (DatabaseRow) -> ()
    ) -> EventLoopFuture<Void> {
        do {
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
                onRow(_MongoDBEntity(
                    document: document,
                    decoder: BSONDecoder(),
                    aggregateQuery: nil
                ))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
