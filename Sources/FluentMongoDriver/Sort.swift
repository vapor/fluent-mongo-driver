import MongoKitten
import FluentKit

extension DatabaseQuery.Sort.Direction {
    internal func makeMongoDirection() throws -> SortOrder {
        switch self {
        case .ascending:
            return .ascending
        case .descending:
            return .descending
        case .custom(let order as SortOrder):
            return order
        case .custom:
            throw FluentMongoError.unsupportedCustomSort
        }
    }
}

extension DatabaseQuery.Sort {
    internal func makeMongoDBSort(aggregate: Bool) throws -> (String, SortOrder) {
        switch self {
        case .sort(let field, let direction):
            let path: String
            
            if aggregate {
                path = try field.makeProjectedMongoPath()
            } else {
                path = try field.makeMongoPath()
            }
            
            return try (path, direction.makeMongoDirection())
        case .custom:
            throw FluentMongoError.unsupportedCustomSort
        }
    }
}
