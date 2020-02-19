import FluentKit
import MongoKitten

extension DatabaseQuery.Filter.Method {
    func makeMongoOperator() throws -> String {
        switch self {
        case .equality(let inverse):
            return inverse ? "$ne" : "$eq"
        case .order(let inverse, let equality):
            switch (inverse, equality) {
            case (false, false):
                return "$gt"
            case (false, true):
                return "$gte"
            case (true, false):
                return "$lt"
            case (true, true):
                return "$lte"
            }
        case .subset:
            return "$in"
        case .custom, .contains:
            throw FluentMongoError.unsupportedOperator
        }
    }
}

extension DatabaseQuery.Filter {
    internal func makeMongoDBFilter() throws -> Document {
        switch self {
        case .value(let field, let operation, let value):
            let path = try field.makeMongoPath()
            let filterOperator = try operation.makeMongoOperator()
            var filter = Document()
            try filter[path][filterOperator] = value.makePrimitive()
            return filter
        case .field:
            throw FluentMongoError.unsupportedFilter
        case .group(let conditions, let relation):
            let conditions = try conditions.map { condition in
                return try condition.makeMongoDBFilter()
            }
            
            switch relation {
            case .and:
                return AndQuery(conditions: conditions).makeDocument()
            case .or:
                return OrQuery(conditions: conditions).makeDocument()
            case .custom:
                throw FluentMongoError.unsupportedCustomFilter
            }
        case .custom(let filter as Document):
            return filter
        case .custom:
            throw FluentMongoError.unsupportedCustomFilter
        }
    }
}
