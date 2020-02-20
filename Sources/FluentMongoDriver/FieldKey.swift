import FluentKit

extension FieldKey {
    func makeMongoKey() -> String {
        switch self {
        case .id:
            return "_id"
        case .string(let name):
            return name
        case .prefixed(let prefix, let key):
            return prefix + key.makeMongoKey()
        case .aggregate:
            fatalError("Unesupported FieldKey: \(self).")
        }
    }
}

extension DatabaseQuery.Field {
    func makeMongoPath() throws -> String {
        switch self {
        case .field(let path, _, _):
            return path.map { $0.makeMongoKey() }.joined(separator: ".")
        case .custom:
            throw FluentMongoError.unsupportedField
        }
    }
    
    func makeProjectedMongoPath() throws -> String {
        switch self {
        case .field(let path, let schema, let alias):
            if let alias = alias {
                return alias
            } else if let schema = schema {
                let path = path.map { $0.makeMongoKey() }.joined(separator: ".")
                return "\(schema).\(path)"
            } else {
                return try makeMongoPath()
            }
        case .custom:
            throw FluentMongoError.unsupportedField
        }
    }
}
