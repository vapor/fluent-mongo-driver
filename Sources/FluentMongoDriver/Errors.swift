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

public enum FluentMongoError: Error, DatabaseError {
    public var isSyntaxError: Bool { false }
    public var isConstraintFailure: Bool { false }
    public var isConnectionClosed: Bool { false }
    
    case missingHosts
    case noTargetDatabaseSpecified
    case unsupportedJoin
    case unsupportedOperator
    case unsupportedFilterValue
    case invalidIndexKey
    case unsupportedField
    case unsupportedDefaultValue
    case insertFailed
    case unsupportedFilter
    case unsupportedCustomLimit
    case unsupportedCustomFilter
    case unsupportedCustomValue
    case unsupportedCustomAction
    case unsupportedCustomSort
    case unsupportedCustomAggregate
    case notMongoDB, fileNotFound
}


extension FluentMongoError: LocalizedError {
    public var errorDescription: String? {
        "\(self)"
    }
}
