import Logging
import NIO
import FluentBenchmark
import FluentMongoDriver
import XCTest

final class DateRange: Model {
    static let schema = "date-range"
    
    @ID(key: .id)
    var id: UUID?
    
    @Field(key: "start")
    var start: Date
    
    @Field(key: "end")
    var end: Date
    
    init() {}
    
    init(from: Date, to: Date) {
        self.start = from
        self.end = to
    }
}

public final class Entity: Model {
    public static let schema = "entities"
    
    @ID(custom: .id)
    public var id: ObjectId?

    @Field(key: "name")
    public var name: String

    public init() { }

    public init(id: ObjectId? = nil, name: String) {
        self.id = id
        self.name = name
    }
}

public final class DocumentStorage: Model {
    public static let schema = "documentstorages"
    
    @ID(custom: .id)
    public var id: ObjectId?

    @Field(key: "document")
    public var document: Document

    public init() { }

    public init(id: ObjectId? = nil, document: Document) {
        self.id = id
        self.document = document
    }
}

public final class Nested: Fields {
    @Field(key: "value")
    public var value: String
    
    public init() {}
    public init(value: String) {
        self.value = value
    }
}

public final class NestedStorage: Model {
    public static let schema = "documentstorages"
    
    @ID(custom: .id)
    public var id: ObjectId?

    @Field(key: "nested")
    public var nested: Nested

    public init() { }

    public init(id: ObjectId? = nil, nested: Nested) {
        self.id = id
        self.nested = nested
    }
}

final class FluentMongoDriverTests: XCTestCase {
    func testAggregate() throws {
        try self.benchmarker.testAggregate(max: false)
    }
    func testArray() throws { try self.benchmarker.testArray() }
    func testBatch() throws { try self.benchmarker.testBatch() }
    func testChildren() throws { try self.benchmarker.testChildren() }
    func testChunk() throws { try self.benchmarker.testChunk() }
    func testCRUD() throws { try self.benchmarker.testCRUD() }
    func testEagerLoad() throws { try self.benchmarker.testEagerLoad() }
    func testEnum() throws { try self.benchmarker.testEnum() }
    func testGroup() throws { try self.benchmarker.testGroup() }
    func testID() throws {
        try self.benchmarker.testID(
            autoincrement: false,
            custom: false
        )
    }
    func testFilter() throws {
        try self.benchmarker.testFilter(sql: false)
    }
    func testJoin() throws { try self.benchmarker.testJoin() }
    func testMiddleware() throws { try self.benchmarker.testMiddleware() }
    func testMigrator() throws { try self.benchmarker.testMigrator() }
    func testModel() throws { try self.benchmarker.testModel() }
    func testOptionalParent() throws { try self.benchmarker.testOptionalParent() }
    func testPagination() throws { try self.benchmarker.testPagination() }
    func testParent() throws { try self.benchmarker.testParent() }
    func testPerformance() throws { try self.benchmarker.testPerformance() }
    func testRange() throws { try self.benchmarker.testRange() }
    func testSet() throws { try self.benchmarker.testSet() }
    func testSiblings() throws { try self.benchmarker.testSiblings() }
    func testSoftDelete() throws { try self.benchmarker.testSoftDelete() }
    func testSort() throws { try self.benchmarker.testSort() }
    func testTimestamp() throws { try self.benchmarker.testTimestamp() }
//    func testTransaction() throws { try self.benchmarker.testTransaction() }
    func testUnique() throws { try self.benchmarker.testUnique() }
    
    func testJoinLimit() throws {
        let migration = SolarSystem()
        try migration.prepare(on: db).wait()
        defer {
            _ = try? migration.revert(on: db).wait()
        }
        
        do {
            let planets = try Planet.query(on: db).all().wait()
            
            guard planets.count > 1, let lastId = planets.last?.id else {
                XCTFail("Invalid dataset for test")
                return
            }

            let planet = try Planet.query(on: db)
                .join(Star.self, on: \Planet.$star.$id == \Star.$id)
                .filter(\.$id == lastId)
                .first()
                .wait()
            
            XCTAssertEqual(planet?.id, lastId)
        } catch {
            XCTFail("\(error)")
        }
    }
    
    func testDate() throws {
        let range = DateRange(from: Date(), to: Date())
        try range.save(on: db).wait()
        
        guard let sameRange = try DateRange.find(range.id, on: db).wait() else {
            XCTFail()
            return
        }
        
        // Dates are doubles, which are not 100% precise. So this fails on Linux.
        XCTAssert(abs(range.start.timeIntervalSince(sameRange.start)) < 0.1)
        XCTAssert(abs(range.end.timeIntervalSince(sameRange.end)) < 0.1)
    }
    
    func testNestedDocuments() throws {
        let doc = DocumentStorage(document: ["key": true])
        try doc.save(on: db).wait()
        
        guard let sameDoc = try DocumentStorage.query(on: db).filter("document.key", .equal, true).first().wait() else {
            XCTFail("Query failed to find the saved entity")
            return
        }
        
        XCTAssertEqual(sameDoc.document["key"] as? Bool, true)
    }
    
    func testNestedFields() throws {
        let doc = NestedStorage(nested: .init(value: "hello"))
        try doc.save(on: db).wait()
        
        guard let sameDoc = try NestedStorage.query(on: db).filter("nested.value", .equal, "hello").first().wait() else {
            XCTFail("Query failed to find the saved entity")
            return
        }
        
        XCTAssertEqual(sameDoc.nested.value, "hello")
    }
  
    func testObjectId() throws {
        let entity = Entity(name: "test")
        
        XCTAssertEqual(try Entity.query(on: db).count().wait(), 0)
        
        try entity.save(on: db).wait()
        XCTAssertEqual(try Entity.query(on: db).count().wait(), 1)
        
        XCTAssertNotNil(try Entity.find(entity.id, on: db).wait())
        
        try entity.delete(on: db).wait()
        XCTAssertEqual(try Entity.query(on: db).count().wait(), 0)
    }
    
    func testGridFS() throws {
        struct JSON: Codable, Equatable {
            let name: String
        }
        
        let writtenEntity = JSON(name: "Hello")
        let writtenData = try JSONEncoder().encode(writtenEntity)
        var buffer = ByteBufferAllocator().buffer(capacity: writtenData.count)
        buffer.writeBytes(writtenData)
        let writtenFile = try GridFSFile.upload(buffer, on: db).wait()
        
        guard let readBuffer = try GridFSFile.read(writtenFile._id, on: db).wait() else {
            XCTFail("File not found")
            return
        }
        
        guard let readBytes = readBuffer.getBytes(at: 0, length: writtenData.count) else {
            XCTFail("Mismatching data")
            return
        }
        
        let readEntity = try JSONDecoder().decode(JSON.self, from: Data(readBytes))
        XCTAssertEqual(writtenEntity, readEntity)
    }
    
    var benchmarker: FluentBenchmarker {
        return .init(databases: self.dbs, (.benchmark1, .benchmark2))
    }
    var eventLoopGroup: EventLoopGroup!
    var threadPool: NIOThreadPool!
    var dbs: Databases!
    var db: Database {
        self.benchmarker.database
    }
    var mongodb: MongoDatabaseRepresentable {
        db as! MongoDatabaseRepresentable
    }
    
    override func setUpWithError() throws {
        try super.setUpWithError()
        
        XCTAssert(isLoggingConfigured)
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.threadPool = NIOThreadPool(numberOfThreads: 1)
        self.dbs = Databases(threadPool: threadPool, on: self.eventLoopGroup)
        
        let hostname = getenv("MONGO_HOSTNAME")
            .flatMap { String(cString: $0) }
            ?? "localhost"
        try self.dbs.use(.mongo(connectionString: "mongodb://\(hostname):27017/vapor-database"), as: .mongo)
        try self.dbs.use(.mongo(connectionString: "mongodb://\(hostname):27017/vapor-benchmark1"), as: .benchmark1)
        try self.dbs.use(.mongo(connectionString: "mongodb://\(hostname):27017/vapor-benchmark2"), as: .benchmark2)

        // Drop existing tables.
        let database1 = try XCTUnwrap(
            self.benchmarker.databases.database(
                .benchmark1,
                logger: Logger(label: "test.fluent.benchmark1"),
                on: self.eventLoopGroup.next()
            ) as? MongoDatabaseRepresentable
        )
        let database2 = try XCTUnwrap(
            self.benchmarker.databases.database(
                .benchmark2,
                logger: Logger(label: "test.fluent.benchmark2"),
                on: self.eventLoopGroup.next()
            ) as? MongoDatabaseRepresentable
        )

        try mongodb.raw.drop().wait()
        try database1.raw.drop().wait()
        try database2.raw.drop().wait()
    }
    
    override func tearDownWithError() throws {
        self.dbs.shutdown()
        try self.threadPool.syncShutdownGracefully()
        try self.eventLoopGroup.syncShutdownGracefully()
        
        try super.tearDownWithError()
    }
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = .debug
        return handler
    }
    return true
}()

extension DatabaseID {
    static let benchmark1 = DatabaseID(string: "benchmark1")
    static let benchmark2 = DatabaseID(string: "benchmark2")
}
