import Foundation
@preconcurrency import MongoKitten
import FluentKit

extension MongoDatabaseRepresentable {
    internal var _gridFS: GridFSBucket {
        GridFSBucket(in: raw)
    }
}

extension GridFSFile {
    public static func find(_ id: any Primitive, on database: any Database) -> EventLoopFuture<GridFSFile?> {
        guard let mongodb = database as? (any MongoDatabaseRepresentable) else {
            return database.eventLoop.makeFailedFuture(FluentMongoError.notMongoDB)
        }
        
        return mongodb._gridFS.findFile(byId: id)
    }
    
    public static func read(_ id: any Primitive, on database: any Database) -> EventLoopFuture<ByteBuffer?> {
        return find(id, on: database).flatMap { file in
            guard let file = file else {
                return database.eventLoop.makeSucceededFuture(nil)
            }
            
            // Map to optional
            return file.reader.readByteBuffer().map { $0 }
        }
    }
    
    public static func upload(
        _ buffer: ByteBuffer,
        named filename: String? = nil,
        metadata: Document? = nil,
        on database: any Database
    ) -> EventLoopFuture<GridFSFile> {
        guard let mongodb = database as? (any MongoDatabaseRepresentable) else {
            return database.eventLoop.makeFailedFuture(FluentMongoError.notMongoDB)
        }
        
        let writer = GridFSFileWriter(toBucket: mongodb._gridFS)
        return writer.write(data: buffer).flatMap {
            return writer.finalize(filename: filename, metadata: metadata)
        }
    }
}
