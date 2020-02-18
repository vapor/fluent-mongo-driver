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
