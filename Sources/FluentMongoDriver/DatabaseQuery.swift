import FluentKit
import MongoKitten

extension DatabaseQuery {
    internal func makeMongoDBSort() throws -> MongoKitten.Sort? {
        var sortSpec = [(String, SortOrder)]()
        
        for sort in sorts {
            switch sort {
            case .sort(let field, let direction):
            switch field {
                case .field(let path, _, _):
                    let path = try path.map { try $0.makeMongoKey() }.joined(separator: ".")
                    try sortSpec.append((path, direction.makeMongoDirection()))
                case .custom, .aggregate:
                    throw FluentMongoError.unsupportedField
                }
            case .custom:
                throw FluentMongoError.unsupportedCustomSort
            }
        }
        
        if sortSpec.isEmpty {
            return nil
        }
        
        return MongoKitten.Sort(sortSpec)
    }
    
    internal func makeMongoDBFilter() throws -> Document {
        var conditions = [Document]()

        for filter in filters {
            conditions.append(try filter.makeMongoDBFilter())
        }
        
        if conditions.isEmpty {
            return [:]
        }
        
        if conditions.count == 1 {
            return conditions[0]
        }
        
        return AndQuery(conditions: conditions).makeDocument()
    }
    
    internal func makeValueDocuments() throws -> [Document] {
        let keys = try fields.map { field -> String in
            switch field {
            case .field(let path, _, _) where path.count == 1:
                return try path[0].makeMongoKey()
            case .aggregate, .custom, .field:
                throw FluentMongoError.unsupportedField
            }
        }
        
        return try input.map { entity -> Document in
            assert(entity.count == keys.count, "The entity's keys.count != values.count")
            
            var document = Document()
            
            for index in 0..<keys.count {
                let key = keys[index]
                let value = try entity[index].makePrimitive()
                
                if key == "_id" {
                    document.insert(value, forKey: "_id", at: 0)
                } else {
                    document.appendValue(value, forKey: key)
                }
            }
            
            return document
        }
    }
}
