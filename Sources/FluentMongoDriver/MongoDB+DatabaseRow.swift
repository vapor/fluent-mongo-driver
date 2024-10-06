@preconcurrency import MongoKitten
import FluentKit

struct _MongoDBAggregateResponse: DatabaseOutput {
    let value: any Primitive
    let decoder: BSONDecoder
    
    var description: String {
        "\(self.value)"
    }

    func schema(_ schema: String) -> any DatabaseOutput {
        self
    }
    
    func contains(_ key: FieldKey) -> Bool {
        key == .aggregate
    }

    func decodeNil(_ key: FieldKey) throws -> Bool {
        false
    }

    func decode<T>(_ key: FieldKey, as type: T.Type) throws -> T
        where T: Decodable
    {
        try self.decoder.decode(type, fromPrimitive: self.value)
    }
}
