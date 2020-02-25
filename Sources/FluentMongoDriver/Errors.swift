import MongoKitten
import MongoCore
import FluentKit
import Foundation

extension MongoWriteError: DatabaseError {
    public var isSyntaxError: Bool { false }
    public var isConstraintFailure: Bool { false }
    public var isConnectionClosed: Bool { false }
}

extension MongoError: DatabaseError {
    public var isSyntaxError: Bool { false }
    public var isConstraintFailure: Bool { false }
    public var isConnectionClosed: Bool { false }
}

enum FluentMongoError: Error, DatabaseError {
    var isSyntaxError: Bool { false }
    var isConstraintFailure: Bool { false }
    var isConnectionClosed: Bool { false }
    
    case missingHosts, noTargetDatabaseSpecified, unsupportedJoin, unsupportedOperator, invalidIndexKey
    case unsupportedField, unsupportedDefaultValue, insertFailed, unsupportedFilter
    case unsupportedCustomLimit, unsupportedCustomFilter, unsupportedCustomValue, unsupportedCustomAction, unsupportedCustomSort, unsupportedCustomAggregate
}


extension FluentMongoError: LocalizedError {
    var errorDescription: String? {
        "\(self)"
    }
}
