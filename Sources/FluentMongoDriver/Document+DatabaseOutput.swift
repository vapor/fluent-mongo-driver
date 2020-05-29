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

    func contains(_ key: FieldKey) -> Bool {
        self.primitive(key) != nil
    }

    func nested(_ key: FieldKey) throws -> DatabaseOutput {
        guard let document = self.primitive(key) as? Document else {
            fatalError()
        }
        return _FluentMongoOutput(document: document, decoder: self.decoder, schema: self.schema)
    }

    func decodeNil(_ key: FieldKey) throws -> Bool {
        if let primitive = self.primitive(key) {
            return primitive.equals(Null())
        } else {
            return true
        }
    }

    func decode<T>(_ key: FieldKey, as type: T.Type) throws -> T
        where T : Decodable
    {
        try self.decoder.decode(
            type,
            fromPrimitive: self.primitive(key) ?? Null()
        )
    }

    private func primitive(_ key: FieldKey) -> Primitive? {
        self.document[key.makeMongoKey()]
    }
}
