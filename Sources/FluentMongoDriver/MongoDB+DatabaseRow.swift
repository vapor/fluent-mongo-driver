import MongoKitten
import FluentKit

struct _MongoDBAggregateResponse: DatabaseOutput {
    let value: Primitive
    let decoder: BSONDecoder
    
    var description: String {
        "\(self.value)"
    }

    func schema(_ schema: String) -> DatabaseOutput {
        self
    }
    
    func contains(_ field: FieldKey) -> Bool {
        field == .aggregate
    }
    
    func decode<T>(_ field: FieldKey, as type: T.Type) throws -> T
        where T: Decodable
    {
        try self.decoder.decode(type, fromPrimitive: value)
    }
}
