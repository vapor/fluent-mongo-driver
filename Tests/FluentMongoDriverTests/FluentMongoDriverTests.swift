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

final class CustomIDEntity: Model {
    static let schema = "entities"
    
    @ID(custom: .id)
    var id: ObjectId?

    @Field(key: "name")
    var name: String

    init() { }

    init(id: ObjectId? = nil, name: String) {
        self.id = id
        self.name = name
    }
}

final class NestedSiblings: Model {
    static let schema = "parent"
    @ID(custom: .id)
    var id: ObjectId?

    @SiblingsField(key: "dates")
    var dates: [DateRange]

    init() { }

    init(id: ObjectId? = nil, dates: [UUID]) {
        self.$dates.identifiers = dates
    }
}

final class DocumentStorage: Model {
    static let schema = "documentstorages"
    
    @ID(custom: .id)
    var id: ObjectId?
    
    @Field(key: "document")
    var document: Document

    init() { }

    init(id: ObjectId? = nil, document: Document) {
        self.id = id
        self.document = document
    }
}

final class Nested: Fields {
    @Field(key: "value")
    var value: String
    
    init() {}
    init(value: String) {
        self.value = value
    }
}

final class NestedStorage: Model {
    static let schema = "documentstorages"
    
    @ID(custom: .id)
    var id: ObjectId?

    @Field(key: "nested")
    var nested: Nested

    init() { }

    init(id: ObjectId? = nil, nested: Nested) {
        self.id = id
        self.nested = nested
    }
}

final class FluentMongoDriverTests: XCTestCase {
    func testAggregate() throws { try self.benchmarker.testAggregate(max: false) }
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
    func testFilter() throws { try self.benchmarker.testFilter(sql: false) }
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
    func testSort() throws { try self.benchmarker.testSort(sql: false) }
    func testTimestamp() throws { try self.benchmarker.testTimestamp() }
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
        let entity = CustomIDEntity(name: "test")
        
        XCTAssertEqual(try CustomIDEntity.query(on: db).count().wait(), 0)
        
        try entity.save(on: db).wait()
        XCTAssertEqual(try CustomIDEntity.query(on: db).count().wait(), 1)
        
        XCTAssertNotNil(try CustomIDEntity.find(entity.id, on: db).wait())
        
        try entity.delete(on: db).wait()
        XCTAssertEqual(try CustomIDEntity.query(on: db).count().wait(), 0)
    }
    
    func testSiblingsField() throws {
        let range = 0..<5
        let siblings = try range.map { _ -> DateRange in
            let range = DateRange(from: Date(), to: Date())
            try range.save(on: db).wait()
            return range
        }
        
        let entity = NestedSiblings(dates: siblings.compactMap(\.id))
        try entity.save(on: db).wait()
        
        // This will work as long as there's one entity
        let _sameEntity = try NestedSiblings.query(on: db).with(\.$dates).first().wait()
        
        guard let sameEntity = _sameEntity else {
            XCTFail("No entities found, although there was one saved")
            return
        }
        
        for i in range {
            XCTAssertEqual(sameEntity.dates[i].id, siblings[i].id)
        }
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
        return .init(databases: self.dbs)
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
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.threadPool = NIOThreadPool(numberOfThreads: System.coreCount)
        self.dbs = Databases(threadPool: threadPool, on: self.eventLoopGroup)

        try self.dbs.use(.mongo(settings: .init(
            authentication: .unauthenticated,
            hosts: [.init(
                hostname: env("MONGO_HOSTNAME_A") ?? "localhost",
                port: env("MONGO_PORT_A").flatMap(Int.init) ?? 27017
            )],
            targetDatabase: env("MONGO_DATABASE_A") ?? "vapor_database"
        )), as: .a)
        try self.dbs.use(.mongo(settings: .init(
            authentication: .unauthenticated,
            hosts: [.init(
                hostname: env("MONGO_HOSTNAME_B") ?? "localhost",
                port: env("MONGO_PORT_B").flatMap(Int.init) ?? 27017
            )],
            targetDatabase: env("MONGO_DATABASE_B") ?? "vapor_database_2"
        )), as: .b)

        // Drop existing tables.
        let a = self.dbs.database(.a, logger: Logger(label: "test.fluent.a"), on: self.eventLoopGroup.next()) as! MongoDatabaseRepresentable
        try a.raw.drop().wait()
        let b = self.dbs.database(.b, logger: Logger(label: "test.fluent.a"), on: self.eventLoopGroup.next()) as! MongoDatabaseRepresentable
        try b.raw.drop().wait()
    }
    
    override func tearDownWithError() throws {
        self.dbs.shutdown()
        try self.threadPool.syncShutdownGracefully()
        try self.eventLoopGroup.syncShutdownGracefully()
        
        try super.tearDownWithError()
    }
}

func env(_ name: String) -> String? {
    return ProcessInfo.processInfo.environment[name]
}

let isLoggingConfigured: Bool = {
    LoggingSystem.bootstrap { label in
        var handler = StreamLogHandler.standardOutput(label: label)
        handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
        return handler
    }
    return true
}()

extension DatabaseID {
    static let a = DatabaseID(string: "mongo-a")
    static let b = DatabaseID(string: "mongo-b")
}
