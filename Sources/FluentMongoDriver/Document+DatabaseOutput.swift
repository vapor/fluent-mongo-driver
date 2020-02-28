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

    func contains(_ path: [FieldKey]) -> Bool {
        self.primitive(path) != nil
    }

    func decode<T>(_ path: [FieldKey], as type: T.Type) throws -> T
        where T : Decodable
    {
        try self.decoder.decode(
            type,
            fromPrimitive: self.primitive(path) ?? Null()
        )
    }

    private func primitive(_ path: [FieldKey]) -> Primitive? {
        var current: Primitive? = self.document
        if let schema = self.schema {
            current = current[schema]
        }
        for field in path {
            let key = field.makeMongoKey()
            current = current[key]
        }
        return current
    }
}
