import MongoKitten
import FluentKit

final class _MongoDBEntity: DatabaseRow {
    let document: Document
    let decoder: BSONDecoder
    let aggregateQuery: DatabaseQuery?
    
    init(
        document: Document,
        decoder: BSONDecoder,
        aggregateQuery: DatabaseQuery?
    ) {
        self.document = document
        self.decoder = decoder
        self.aggregateQuery = aggregateQuery
    }
    
    var description: String { document.debugDescription }
    
    func contains(field: FieldKey) -> Bool {
        do {
            if let aggregateQuery = self.aggregateQuery {
                if case .prefixed(var prefix, let key) = field, prefix.count > 0 {
                    prefix.removeLast()
                    return try document[prefix][key.makeMongoKey()] != nil
                }
                
                let key = try field.makeMongoKey()
                
                nextField: for field in aggregateQuery.fields {
                    guard case .field(let name, let schema, let alias) = field else {
                        throw FluentMongoError.unsupportedField
                    }
                    
                    let firstKey = try? name.first?.makeMongoKey()
                    if key != firstKey {
                        continue nextField
                    }
                    
                    if let output = alias ?? schema {
                        return document[output][key] != nil
                    } else {
                        return document[aggregateQuery.schema][key] != nil
                    }
                }
                
                return false
            } else {
                let key = try field.makeMongoKey()
                return document[key] != nil
            }
        } catch {
            return false
        }
    }
    
    func decode<T>(field: FieldKey, as type: T.Type, for database: Database) throws -> T where T : Decodable {
        var value: Primitive = Null()
        
        findValue: if let aggregateQuery = self.aggregateQuery {
            if case .prefixed(var prefix, let key) = field, prefix.count > 0 {
                prefix.removeLast()
                
                if let _value = try document[prefix][key.makeMongoKey()] {
                    value = _value
                    break findValue
                }
            }
            
            let key = try field.makeMongoKey()
            
            nextField: for field in aggregateQuery.fields {
                guard case .field(let name, let schema, let alias) = field else {
                    throw FluentMongoError.unsupportedField
                }
                
                let firstKey = try? name.first?.makeMongoKey()
                if key != firstKey {
                    continue nextField
                }
                
                if let output = alias ?? schema {
                    value = document[output][key] ?? Null()
                } else {
                    value = document[aggregateQuery.schema][key] ?? Null()
                }
                
                break findValue
            }
        } else {
            let key = try field.makeMongoKey()
            value = document[key] ?? Null()
        }
        
        return try decoder.decode(
            type,
            fromPrimitive: value
        )
    }
}

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
