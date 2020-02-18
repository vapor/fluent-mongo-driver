import FluentKit

extension FieldKey {
    func makeMongoKey() throws -> String {
        switch self {
        case .id:
            return "_id"
        case .name(let name):
            return name
        case .prefixed:
            throw FluentMongoError.unsupportedJoin
        }
    }
}
