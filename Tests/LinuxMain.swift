import XCTest
import FluentMongoDriver

var tests = [XCTestCaseEntry]()
tests += FluentMongoDriverTests.allTests()
XCTMain(tests)
