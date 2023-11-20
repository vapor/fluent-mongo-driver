import FluentKit
import MongoKitten

extension DatabaseQuery {
    internal func makeMongoDBSort(aggregate: Bool) throws -> MongoKitten.Sort? {
        var sortSpec = [(String, SortOrder)]()
        
        for sort in sorts {
            sortSpec.append(try sort.makeMongoDBSort(aggregate: aggregate))
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
        try self.input.map { entity -> Document in
            try entity.makePrimitive() as! Document
        }
    }
}
