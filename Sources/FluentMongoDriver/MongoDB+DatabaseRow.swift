import MongoKitten
import FluentKit

struct _MongoDBAggregateResponse: DatabaseRow {
    let value: Primitive
    let decoder: BSONDecoder
    
    var description: String { String(describing: value) }
    
    func contains(field: FieldKey) -> Bool {
        return field == .aggregate
    }
    
    func decode<T>(field: FieldKey, as type: T.Type, for database: Database) throws -> T where T : Decodable {
        try decoder.decode(type, fromPrimitive: value)
    }
}
