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
                let row = FluentMongoRow(
                    document: document,
                    decoder: BSONDecoder()
                )
                onRow(row)
            }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}

private struct FluentMongoRow: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder

    init(
        document: Document,
        decoder: BSONDecoder
    ) {
        self.document = document
        self.decoder = decoder
    }

    var description: String {
        self.document.debugDescription
    }

    func contains(field: FieldKey) -> Bool {
        self.primitive(field: field) != nil
    }

    func decode<T>(field: FieldKey, as type: T.Type, for database: Database) throws -> T
        where T : Decodable
    {
        try self.decoder.decode(
            type,
            fromPrimitive: self.primitive(field: field) ?? Null()
        )
    }

    private func primitive(field: FieldKey) -> Primitive? {
        self.document[field.makeMongoKey()]
    }
}
