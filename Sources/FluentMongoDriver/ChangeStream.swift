import MongoKitten
import FluentKit

extension Model {
    public static func watch(on database: Database, options: ChangeStreamOptions = .init()) -> EventLoopFuture<ChangeStream<Self>> {
        guard let mongodb = database as? MongoDatabaseRepresentable else {
            return database.eventLoop.makeFailedFuture(FluentMongoError.notMongoDB)
        }
        
        return mongodb.raw[Self.schema].watch(options: options, as: Self.self)
    }
}
