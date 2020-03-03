import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        do {
            let transactionDatabase = try mongoKitten.startTransaction(autoCommitChanges: false)
            let database = FluentMongoDatabase(
                cluster: self.cluster,
                mongoKitten: transactionDatabase,
                context: self.context
            )
            return closure(database).flatMap { value in
                transactionDatabase.commit().map { value }
            }.flatMapError { error in
                return transactionDatabase.abort().flatMapThrowing { _ in
                    throw error
                }
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
