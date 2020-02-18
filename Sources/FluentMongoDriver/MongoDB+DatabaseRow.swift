import MongoKitten
import FluentKit

struct _MongoDBEntity: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder
    
    var description: String { document.debugDescription }
    
    func contains(field: FieldKey) -> Bool {
        do {
            return try document.containsKey(field.makeMongoKey())
        } catch {
            return false
        }
    }
    
    func decode<T>(field: FieldKey, as type: T.Type, for database: Database) throws -> T where T : Decodable {
        try decoder.decode(type, fromPrimitive: document[field.makeMongoKey()] ?? Null())
    }
}
