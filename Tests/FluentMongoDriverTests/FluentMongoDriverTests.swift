import Logging
import NIO
import FluentBenchmark
import FluentMongoDriver
import XCTest

final class FluentMongoDriverTests: XCTestCase {
    // func testAll() throws { try self.benchmarker.testAll() }
    // func testAggregate() throws { try self.benchmarker.testAggregate() }
    func testArray() throws { try self.benchmarker.testArray() }
    func testBatch() throws { try self.benchmarker.testBatch() }
    func testChildren() throws { try self.benchmarker.testChildren() }
    func testChunk() throws { try self.benchmarker.testChunk() }
    func testCompoundField() throws { try self.benchmarker.testCompoundField() }
    // func testCRUD() throws { try self.benchmarker.testCRUD() }
    // func testEagerLoad() throws { try self.benchmarker.testEagerLoad() }
    func testEnum() throws { try self.benchmarker.testEnum() }
    // func testFilter() throws { try self.benchmarker.testFilter() }
    func testGroup() throws { try self.benchmarker.testGroup() }
    // func testID() throws { try self.benchmarker.testID() }
    // func testJoin() throws { try self.benchmarker.testJoin() }
    func testMiddleware() throws { try self.benchmarker.testMiddleware() }
    func testMigrator() throws { try self.benchmarker.testMigrator() }
    func testModel() throws { try self.benchmarker.testModel() }
    func testNestedField() throws { try self.benchmarker.testNestedField() }
    func testOptionalParent() throws { try self.benchmarker.testOptionalParent() }
    // func testPagination() throws { try self.benchmarker.testPagination() }
    func testParent() throws { try self.benchmarker.testParent() }
    func testPerformance() throws { try self.benchmarker.testPerformance() }
    // func testRange() throws { try self.benchmarker.testRange() }
    func testSet() throws { try self.benchmarker.testSet() }
    // func testSiblings() throws { try self.benchmarker.testSiblings() }
    func testSoftDelete() throws { try self.benchmarker.testSoftDelete() }
    // func testSort() throws { try self.benchmarker.testSort() }
    func testTimestamp() throws { try self.benchmarker.testTimestamp() }
    // func testTransaction() throws { try self.benchmarker.testTransaction() }
    func testUnique() throws { try self.benchmarker.testUnique() }
    
    var benchmarker: FluentBenchmarker {
        return .init(databases: self.dbs)
    }
    var eventLoopGroup: EventLoopGroup!
    var threadPool: NIOThreadPool!
    var dbs: Databases!
    var db: Database {
        self.benchmarker.database
    }
    
    override func setUp() {
        XCTAssert(isLoggingConfigured)
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.threadPool = NIOThreadPool(numberOfThreads: 1)
        self.dbs = Databases(threadPool: threadPool, on: self.eventLoopGroup)
        let hostname = getenv("MONGO_HOSTNAME")
            .flatMap { String(cString: $0) }
            ?? "localhost"
        try! self.dbs.use(.mongo(connectionString: "mongodb://\(hostname):27017/vapor-database"), as: .mongo)
        // Drop existing tables.
        try! (self.db as! MongoDatabaseRepresentable).raw.drop().wait()
    }

    override func tearDown() {
        self.dbs.shutdown()
        try! self.threadPool.syncShutdownGracefully()
        try! self.eventLoopGroup.syncShutdownGracefully()
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
