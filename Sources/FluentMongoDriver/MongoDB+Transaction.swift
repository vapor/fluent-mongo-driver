import FluentKit
import MongoKitten
import MongoCore

extension FluentMongoDatabase {
    func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T> {
        guard !self.inTransaction else {
            return closure(self)
        }
        do {
            let transactionDatabase = try raw.startTransaction(autoCommitChanges: false)
            let database = FluentMongoDatabase(
                cluster: self.cluster,
                raw: transactionDatabase,
                context: self.context,
                inTransaction: true
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
