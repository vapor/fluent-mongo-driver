import FluentKit
import MongoKitten
import MongoCore

extension _MongoDB {
    func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        do {
            let transactionDatabase = try raw.startTransaction(autoCommitChanges: false)
            let database = _MongoDB(
                cluster: self.cluster,
                database: transactionDatabase
            )
            return closure(database).flatMap { value in
                transactionDatabase.commit().map { value }
            }.flatMapError { error in
                transactionDatabase.abort().flatMapThrowing { _ in
                    throw error
                }
            }
        } catch {
            return self.eventLoop.makeFailedFuture(error)
        }
    }
}
