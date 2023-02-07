import FluentKit

extension FieldKey {
    func makeMongoKey() -> String {
        switch self {
        case .id:
            return "_id"
        case .string(let name):
            return name
        case .aggregate:
            fatalError("Unsupported field key: \(self).")
        case .prefix(let prefix, let key):
            return prefix.makeMongoKey() + key.makeMongoKey()
        }
    }
}

extension DatabaseQuery.Field {
    func makeMongoPath() throws -> String {
        switch self {
        case .path(let path, _), .extendedPath(let path, _, _):
            return path.map { $0.makeMongoKey() }.joined(separator: ".")
        case .custom:
            throw FluentMongoError.unsupportedField
        }
    }

    func makeProjectedMongoPath() throws -> String {
        switch self {
        case .path(let path, let schema), .extendedPath(let path, let schema, nil):
            return "\(schema).\(path.map { $0.makeMongoKey() }.joined(separator: "."))"
        case .extendedPath(_, _, _), .custom:
            throw FluentMongoError.unsupportedField
        }
    }
}
