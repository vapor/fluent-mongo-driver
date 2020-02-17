import XCTest

import FluentMongo

var tests = [XCTestCaseEntry]()
tests += FluentMongoDBTests.allTests()
XCTMain(tests)
