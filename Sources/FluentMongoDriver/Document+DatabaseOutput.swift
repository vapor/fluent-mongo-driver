import FluentKit
import MongoKitten

extension Document {
    internal func databaseOutput(using decoder: BSONDecoder) -> DatabaseOutput {
        _FluentMongoOutput(document: self, decoder: decoder, schema: nil)
    }
}

private struct _FluentMongoOutput: DatabaseOutput {
    let document: Document
    let decoder: BSONDecoder
    let schema: String?

    var description: String {
        self.document.debugDescription
    }

    func schema(_ schema: String) -> DatabaseOutput {
        _FluentMongoOutput(document: self.document, decoder: self.decoder, schema: schema)
    }

    func contains(_ field: FieldKey) -> Bool {
        self.primitive(field: field) != nil
    }

    func decode<T>(_ field: FieldKey, as type: T.Type) throws -> T
        where T : Decodable
    {
        try self.decoder.decode(
            type,
            fromPrimitive: self.primitive(field: field) ?? Null()
        )
    }

    private func primitive(field: FieldKey) -> Primitive? {
        let key = field.makeMongoKey()
        if let schema = self.schema {
            return self.document[schema][key]
        } else {
            return self.document[key]
        }
    }
}
