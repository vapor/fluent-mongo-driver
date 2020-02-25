import FluentKit
import MongoKitten

extension DatabaseQuery {
    internal func makeMongoDBSort() throws -> MongoKitten.Sort? {
        var sortSpec = [(String, SortOrder)]()
        
        for sort in sorts {
            switch sort {
            case .sort(let field, let direction):
                let path = try field.makeMongoPath()
                try sortSpec.append((path, direction.makeMongoDirection()))
            case .custom:
                throw FluentMongoError.unsupportedCustomSort
            }
        }
        
        if sortSpec.isEmpty {
            return nil
        }
        
        return MongoKitten.Sort(sortSpec)
    }
    
    internal func makeMongoDBFilter(aggregate: Bool) throws -> Document {
        var conditions = [Document]()

        for filter in filters {
            conditions.append(try filter.makeMongoDBFilter(aggregate: aggregate))
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
            case .field(let key, _):
                return key.makeMongoKey()
            case .custom:
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
