import Logging
import NIO
import FluentBenchmark
import FluentMongoDriver
import XCTest

final class FluentMongoDriverTests: XCTestCase {
    func testAll() throws {
        try self.benchmarker.testAll()
    }

    func testCreate() throws {
        try self.benchmarker.testCreate()
    }

    func testRead() throws {
        try self.benchmarker.testRead()
    }

    func testUpdate() throws {
        try self.benchmarker.testUpdate()
    }

    func testDelete() throws {
        try self.benchmarker.testDelete()
    }

    func testEagerLoadChildren() throws {
        try self.benchmarker.testEagerLoadChildren()
    }

    func testEagerLoadParent() throws {
        try self.benchmarker.testEagerLoadParent()
    }

    func testEagerLoadParentJSON() throws {
        try self.benchmarker.testEagerLoadParentJSON()
    }

    func testEagerLoadChildrenJSON() throws {
        try self.benchmarker.testEagerLoadChildrenJSON()
    }

    func testMigrator() throws {
        try self.benchmarker.testMigrator()
    }

    func testMigratorError() throws {
        try self.benchmarker.testMigratorError()
    }

    func testJoin() throws {
        try self.benchmarker.testJoin()
    }

    func testBatchCreate() throws {
        try self.benchmarker.testBatchCreate()
    }

    func testBatchUpdate() throws {
        try self.benchmarker.testBatchUpdate()
    }

    func testNestedModel() throws {
        try self.benchmarker.testNestedModel()
    }

    func testAggregates() throws {
        try self.benchmarker.testAggregates()
    }

    func testIdentifierGeneration() throws {
        try self.benchmarker.testIdentifierGeneration()
    }

    func testNullifyField() throws {
        try self.benchmarker.testNullifyField()
    }

    func testChunkedFetch() throws {
        try self.benchmarker.testChunkedFetch()
    }

    func testUniqueFields() throws {
        try self.benchmarker.testUniqueFields()
    }

    func testAsyncCreate() throws {
        try self.benchmarker.testAsyncCreate()
    }

    func testSoftDelete() throws {
        try self.benchmarker.testSoftDelete()
    }

    func testTimestampable() throws {
        try self.benchmarker.testTimestampable()
    }

    func testModelMiddleware() throws {
        try self.benchmarker.testModelMiddleware()
    }

    func testSort() throws {
        try self.benchmarker.testSort()
    }

    func testUUIDModel() throws {
        try self.benchmarker.testUUIDModel()
    }

    func testNewModelDecode() throws {
        try self.benchmarker.testNewModelDecode()
    }

    func testSiblingsAttach() throws {
        try self.benchmarker.testSiblingsAttach()
    }

    func testSiblingsEagerLoad() throws {
        try self.benchmarker.testSiblingsEagerLoad()
    }

    func testParentGet() throws {
        try self.benchmarker.testParentGet()
    }

    func testParentSerialization() throws {
        try self.benchmarker.testParentSerialization()
    }

    func testMultipleJoinSameTable() throws {
        try self.benchmarker.testMultipleJoinSameTable()
    }

    func testOptionalParent() throws {
        try self.benchmarker.testOptionalParent()
    }

    func testFieldFilter() throws {
        try self.benchmarker.testFieldFilter()
    }

    func testJoinedFieldFilter() throws {
        try self.benchmarker.testJoinedFieldFilter()
    }

    func testSameChildrenFromKey() throws {
        try self.benchmarker.testSameChildrenFromKey()
    }

    func testArray() throws {
        try self.benchmarker.testArray()
    }

    func testPerformance() throws {
        try self.benchmarker.testPerformance()
    }

    func testSoftDeleteWithQuery() throws {
        try self.benchmarker.testSoftDeleteWithQuery()
    }

    func testDuplicatedUniquePropertyName() throws {
        try self.benchmarker.testDuplicatedUniquePropertyName()
    }
    
    func testEmptyEagerLoadChildren() throws {
        try self.benchmarker.testEmptyEagerLoadChildren()
    }
    
    func testUInt8BackedEnum() throws {
        try self.benchmarker.testUInt8BackedEnum()
    }

    func testMultipleSet() throws {
        try self.benchmarker.testMultipleSet()
    }

//    func testTransaction() throws {
//        try self.benchmarker.testTransaction()
//    }

    func testPagination() throws {
        try self.benchmarker.testPagination()
    }

    func testBlob() throws {
        final class Foo: Model {
            static let schema = "foos"

            @ID(key: "id")
            var id: Int?

            @Field(key: "data")
            var data: [UInt8]

            init() { }
        }

        struct CreateFoo: Migration {
            func prepare(on database: Database) -> EventLoopFuture<Void> {
                return database.schema("foos")
                    .field("id", .int, .identifier(auto: true))
                    .field("data", .data, .required)
                    .create()
            }

            func revert(on database: Database) -> EventLoopFuture<Void> {
                return database.schema("foos").delete()
            }
        }

        try CreateFoo().prepare(on: self.db).wait()
        try CreateFoo().revert(on: self.db).wait()
    }
    
    var benchmarker: FluentBenchmarker {
        return .init(database: self.db)
    }
    var eventLoopGroup: EventLoopGroup!
    var threadPool: NIOThreadPool!
    var dbs: Databases!
    var db: Database {
        self.dbs.database(logger: .init(label: "codes.vapor.test"), on: self.eventLoopGroup.next())!
    }
    
    override func setUp() {
        let jsonEncoder = JSONEncoder()
        jsonEncoder.dateEncodingStrategy = .iso8601

        let jsonDecoder = JSONDecoder()
        jsonDecoder.dateDecodingStrategy = .iso8601

        XCTAssert(isLoggingConfigured)
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.threadPool = NIOThreadPool(numberOfThreads: 1)
        self.dbs = Databases(threadPool: threadPool, on: self.eventLoopGroup)
        try! self.dbs.use(.mongo(connectionString: "mongodb://localhost/vapor-test"), as: .mongo)
    }

    override func tearDown() {
        let driver = self.dbs.driver() as! MongoDB
        try! driver.raw.drop().wait()
        
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

extension DatabaseID {
    public static var mongo: DatabaseID {
        return .init(string: "mongo")
    }
}
