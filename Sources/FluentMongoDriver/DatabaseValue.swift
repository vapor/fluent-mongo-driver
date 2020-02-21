import FluentKit
import MongoKitten

extension DatabaseQuery.Value {
    func makePrimitive() throws -> Primitive {
        switch self {
        case .array(let values):
            var array = Document(isArray: true)
            for value in values {
                try array.append(value.makePrimitive())
            }
            return array
        case .bind(let value):
            return try BSONEncoder().encodePrimitive(value) ?? Null()
        case .dictionary(let dict):
            var document = Document()
            
            for (key, value) in dict {
                document[key] = try value.makePrimitive()
            }
            
            return document
        case .null:
            return Null()
        case .default:
            throw FluentMongoError.unsupportedDefaultValue
        case .custom(let primitive as Primitive):
            return primitive
        case .enumCase(let string):
            return string
        case .custom:
            throw FluentMongoError.unsupportedCustomValue
        }
    }
}
