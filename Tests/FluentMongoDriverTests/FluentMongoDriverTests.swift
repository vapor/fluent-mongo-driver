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
    
    var benchmarker: FluentBenchmarker {
        return .init(databases: self.dbs)
    }
    var eventLoopGroup: EventLoopGroup!
    var threadPool: NIOThreadPool!
    var dbs: Databases!
    var db: Database {
        self.benchmarker.database
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
        // Drop existing tables.
        try (self.db as! MongoDatabaseRepresentable).raw.drop().wait()
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
