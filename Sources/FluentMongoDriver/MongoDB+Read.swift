import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func read(
        query: DatabaseQuery,
        onOutput: @escaping (DatabaseOutput) -> ()
    ) -> EventLoopFuture<Void> {
        do {
            let condition = try query.makeMongoDBFilter(aggregate: false)
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
            
            var projection = Projection(document: [:])
            for field in query.fields {
                // The entity is not put into the projected path
                // Therefore the standard full path it used without the schema as prefix
                try projection.include(field.makeMongoPath())
            }
            find.command.projection = projection.document
            
            find.command.sort = try query.makeMongoDBSort(aggregate: false)?.document
            
            logger.debug("fluent-mongo find command=\(find.command)")
            let decoder = BSONDecoder()
            return find.forEach { document in
                var wrapped = Document()
                wrapped[query.schema] = document
                onOutput(wrapped.databaseOutput(using: decoder))
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
