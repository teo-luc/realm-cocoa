////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#if canImport(Combine)
import XCTest
import Combine
import RealmSwift

class BackLink: Object {
    let list = List<Linked>()
}
class Linked: Object {
    let backlink = LinkingObjects(fromType: BackLink.self, property: "list")
}

// XCTest doesn't care about the @available on the class and will try to run
// the tests even on older versions. Putting this check inside `defaultTestSuite`
// results in a warning about it being redundant due to the enclosing check, so
// it needs to be out of line.
func hasCombine() -> Bool {
    if #available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *) {
        return true
    }
    return false
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
class CombinePublisherTests: TestCase {
    var realm: Realm! = nil
    var token: AnyCancellable? = nil

    override class var defaultTestSuite: XCTestSuite {
        if hasCombine() {
            return super.defaultTestSuite
        }
        return XCTestSuite(name: "CombinePublisherTests")
    }

    override func setUp() {
        super.setUp()
        realm = try! Realm(configuration: Realm.Configuration(inMemoryIdentifier: "test"))
    }

    override func tearDown() {
        realm = nil
        if let token = token {
            token.cancel()
        }
        super.tearDown()
    }

    func testObjectChange() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }
        let exp = XCTestExpectation()

        token = RealmPublisher(obj).sink { o in
            XCTAssertEqual(obj, o)
            exp.fulfill()
        }

        try! realm.write { obj.intCol = 1 }
        wait(for: [exp], timeout: 1)
    }

    func testObjectChangeSet() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }
        let exp = XCTestExpectation()

        token = ObjectChangePublisher(obj).sink { change in
            if case .change(let o, let properties) = change {
                XCTAssertEqual(obj, o)
                XCTAssertEqual(properties.count, 1)
                XCTAssertEqual(properties[0].name, "intCol")
                XCTAssertNil(properties[0].oldValue)
                XCTAssertEqual(properties[0].newValue as? Int, 1)
            }
            else {
                XCTFail("Expected .change but got \(change)")
            }
            exp.fulfill()
        }

        try! realm.write { obj.intCol = 1 }
        wait(for: [exp], timeout: 1)
    }

    func testObjectDelete() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }
        let exp = XCTestExpectation()

        token = RealmPublisher(obj).sink(receiveCompletion: { _ in exp.fulfill() },
                                         receiveValue: { _ in })

        try! realm.write { realm.delete(obj) }
        wait(for: [exp], timeout: 1)
    }

    func testObjectSubscribeOn() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        let sema = DispatchSemaphore(value: 0)
        var i = 1
        token = RealmPublisher(obj)
            .subscribe(on: DispatchQueue(label: "scheduler queue"))
            .map { obj -> SwiftIntObject in
                sema.signal()
                XCTAssertEqual(obj.intCol, i)
                i += 1
                return obj
            }
            .collect()
            .sink { arr in
                XCTAssertEqual(arr.count, 10)
                sema.signal()
            }

        for _ in 0..<10 {
            try! realm.write { obj.intCol += 1 }
            // wait between each write so that the notifications can't get coalesced
            // also would deadlock if the subscription was on the main thread
            sema.wait()
        }
        try! realm.write { realm.delete(obj) }
        sema.wait()
    }

    func testObjectReceiveOn() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        var exp = XCTestExpectation()
        token = RealmPublisher(obj)
            .receive(on: DispatchQueue(label: "queue"))
            .map { obj -> Int in
                exp.fulfill()
                return obj.intCol
            }
            .collect()
            .sink { arr in
                XCTAssertEqual(arr.count, 10)
                for i in 1..<10 {
                    XCTAssertTrue(arr.contains(i))
                }
                exp.fulfill()
            }

        for _ in 0..<10 {
            try! realm.write { obj.intCol += 1 }
            wait(for: [exp], timeout: 10)
            exp = XCTestExpectation()
        }
        try! realm.write { realm.delete(obj) }
        wait(for: [exp], timeout: 10)
    }

    func testObjectChangeSetSubscribeOn() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }
        var exp = XCTestExpectation(description: "change")

        var prev: SwiftIntObject?
        token = ObjectChangePublisher(obj)
            .subscribe(on: DispatchQueue(label: "queue"))
            .sink(receiveCompletion: { _ in exp.fulfill() }) { change in
                if case .change(let o, let properties) = change {
                    XCTAssertNotEqual(obj, o)
                    XCTAssertEqual(properties.count, 1)
                    XCTAssertEqual(properties[0].name, "intCol")
                    if let prev = prev {
                        XCTAssertEqual(properties[0].oldValue as? Int, prev.intCol)
                    }
                    XCTAssertEqual(properties[0].newValue as? Int, o.intCol)
                    prev = o.freeze()
                    XCTAssertEqual(prev!.intCol, o.intCol)

                    if o.intCol == 100 {
                        exp.fulfill()
                    }
                }
                else {
                    XCTFail("Expected .change but got \(change)")
                }
            }

        for _ in 0..<100 {
            try! realm.write { obj.intCol += 1 }
        }
        wait(for: [exp], timeout: 1)
        exp = XCTestExpectation(description: "completion")
        try! realm.write { realm.delete(obj) }
        
        wait(for: [exp], timeout: 10)
        XCTAssertNotNil(prev)
        XCTAssertEqual(prev!.intCol, 100)
    }

    func testObjectChangeSetReceiveOn() {
        // Not implemented
    }

    func testFrozenObject() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }
        let exp = XCTestExpectation()

        token = RealmPublisher(obj, freeze: true).collect().sink { arr in
            XCTAssertEqual(arr.count, 10)
            for i in 0..<10 {
                XCTAssertEqual(arr[i].intCol, i + 1)
            }
            exp.fulfill()
        }

        for _ in 0..<10 {
            try! realm.write { obj.intCol += 1 }
        }
        try! realm.write { realm.delete(obj) }
        wait(for: [exp], timeout: 1)
    }

    func testFrozenObjectChangeSetSubscribeOn() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        let sema = DispatchSemaphore(value: 0)
        token = ObjectChangePublisher(obj, freeze: true)
            .subscribe(on: DispatchQueue(label: "queue"))
            .collect()
            .sink { arr in
                var prev: SwiftIntObject?
                for change in arr {
                    guard case .change(let obj, let properties) = change else {
                        XCTFail("Expected .change, got(\(change)")
                        sema.signal()
                        return
                    }

                    XCTAssertEqual(properties.count, 1)
                    XCTAssertEqual(properties[0].name, "intCol")
                    XCTAssertEqual(properties[0].newValue as? Int, obj.intCol)
                    if let prev = prev {
                        XCTAssertEqual(properties[0].oldValue as? Int, prev.intCol)
                    } else {
                        XCTAssertNil(properties[0].oldValue)
                    }
                    prev = obj
                }
                sema.signal()
            }

        for _ in 0..<100 {
            try! realm.write { obj.intCol += 1 }
        }
        try! realm.write { realm.delete(obj) }
        sema.wait()
    }

    func testFrozenObjectChangeSetReceiveOn() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        let exp = XCTestExpectation(description: "sink complete")
        token = ObjectChangePublisher(obj, freeze: true)
            .receive(on: DispatchQueue(label: "queue"))
            .collect()
            .sink { arr in
                for change in arr {
                    guard case .change(let obj, let properties) = change else {
                        XCTFail("Expected .change, got(\(change)")
                        exp.fulfill()
                        return
                    }

                    XCTAssertEqual(properties.count, 1)
                    XCTAssertEqual(properties[0].name, "intCol")
                    XCTAssertEqual(properties[0].newValue as? Int, obj.intCol)
                    // subscribing on the thread making writes means that oldValue
                    // is always nil
                    XCTAssertNil(properties[0].oldValue)
                }
                exp.fulfill()
        }

        for _ in 0..<100 {
            try! realm.write { obj.intCol += 1 }
        }
        try! realm.write { realm.delete(obj) }
        wait(for: [exp], timeout: 1)
    }

    func testFrozenObjectChangeSetSubscribOnAndReceiveOn() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        let sema = DispatchSemaphore(value: 0)
        token = ObjectChangePublisher(obj, freeze: true)
            .subscribe(on: DispatchQueue(label: "subscribe queue"))
            .receive(on: DispatchQueue(label: "receive queue"))
            .collect()
            .sink { arr in
                var prev: SwiftIntObject?
                for change in arr {
                    guard case .change(let obj, let properties) = change else {
                        XCTFail("Expected .change, got(\(change)")
                        sema.signal()
                        return
                    }

                    XCTAssertEqual(properties.count, 1)
                    XCTAssertEqual(properties[0].name, "intCol")
                    XCTAssertEqual(properties[0].newValue as? Int, obj.intCol)
                    if let prev = prev {
                        XCTAssertEqual(properties[0].oldValue as? Int, prev.intCol)
                    }
                    prev = obj
                }
                sema.signal()
        }

        for _ in 0..<100 {
            try! realm.write { obj.intCol += 1 }
        }
        try! realm.write { realm.delete(obj) }
        sema.wait()
    }

    func testReceiveOnAfterMap() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        var exp = XCTestExpectation()
        token = RealmPublisher(obj)
            .map { $0 }
            .makeThreadSafe()
            .receive(on: DispatchQueue(label: "queue"))
            .map { obj -> Int in
                exp.fulfill()
                return obj.intCol
            }
            .collect()
            .sink { arr in
                XCTAssertEqual(arr.count, 10)
                for i in 1..<10 {
                    XCTAssertTrue(arr.contains(i))
                }
                exp.fulfill()
            }

        for _ in 0..<10 {
            try! realm.write { obj.intCol += 1 }
            wait(for: [exp], timeout: 1)
            exp = XCTestExpectation()
        }
        try! realm.write { realm.delete(obj) }
        wait(for: [exp], timeout: 1)
    }

    func testUnmanagedMakeThreadSafe() {
        let objects = [SwiftIntObject(value: [1]), SwiftIntObject(value: [2]), SwiftIntObject(value: [3])]

        let exp = XCTestExpectation()
        token = objects.publisher
            .makeThreadSafe()
            .receive(on: DispatchQueue(label: "queue"))
            .map { $0.intCol }
            .collect()
            .sink { (arr: [Int]) in
                XCTAssertEqual(arr, [1, 2, 3])
                exp.fulfill()
        }
        wait(for: [exp], timeout: 1)
    }

    func testManagedMakeThreadSafe() {
        let objects = try! realm.write {
            return [
                realm.create(SwiftIntObject.self, value: [1]),
                realm.create(SwiftIntObject.self, value: [2]),
                realm.create(SwiftIntObject.self, value: [3])
            ]
        }

        let exp = XCTestExpectation()
        token = objects.publisher
            .makeThreadSafe()
            .receive(on: DispatchQueue(label: "queue"))
            .map { $0.intCol }
            .collect()
            .sink { (arr: [Int]) in
                XCTAssertEqual(arr, [1, 2, 3])
                exp.fulfill()
        }
        wait(for: [exp], timeout: 1)
    }

    func testFrozenMakeThreadSafe() {
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        var exp = XCTestExpectation()
        token = RealmPublisher(obj, freeze: true)
            .map { $0 }
            .makeThreadSafe()
            .receive(on: DispatchQueue(label: "queue"))
            .sink { obj in
                XCTAssertTrue(obj.isFrozen)
                exp.fulfill()
            }

        for _ in 0..<10 {
            try! realm.write { obj.intCol += 1 }
            wait(for: [exp], timeout: 1)
            exp = XCTestExpectation()
        }
    }

    func testMixedMakeThreadSafe() {
        let realm2 = try! Realm(configuration: Realm.Configuration(inMemoryIdentifier: "test2"))
        var objects = try! realm.write {
            try! realm2.write {
                return [
                    realm.create(SwiftIntObject.self, value: [1]),
                    realm2.create(SwiftIntObject.self, value: [2]),
                    SwiftIntObject(value: [3]),
                    realm.create(SwiftIntObject.self, value: [4]),
                    realm2.create(SwiftIntObject.self, value: [5])
                ]
            }
        }
        objects[3] = objects[3].freeze()
        objects[4] = objects[4].freeze()
        let exp = XCTestExpectation()
        token = objects.publisher
            .makeThreadSafe()
            .receive(on: DispatchQueue(label: "queue"))
            .map { $0.intCol }
            .collect()
            .sink { (arr: [Int]) in
                XCTAssertEqual(arr, [1, 2, 3, 4, 5])
                exp.fulfill()
        }
        wait(for: [exp], timeout: 1)
    }


    /*
    func testList() {
        let exp = XCTestExpectation(description: "sink will receive objects")
        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }

        let cancellable = obj.arrayBool.objectWillChange.sink {
            XCTAssertEqual(obj.arrayBool, $0)
            exp.fulfill()
        }

        try! realm.write { obj.arrayBool.append(false) }

        wait(for: [exp], timeout: 10)
        cancellable.cancel()
    }

    func testResultsWillChange() {
        let exp = XCTestExpectation(description: "sink will receive objects")

        let obj = try! realm.write { realm.create(SwiftIntObject.self, value: []) }
        let results = obj.arrayCol.filter("int64Col == 4")

        let cancellable = results.objectWillChange.sink {
            XCTAssertEqual(obj.arrayCol[0], $0[0])
            exp.fulfill()
        }

        try! realm.write { obj.arrayCol.append(SwiftKVOObject()) }

        wait(for: [exp], timeout: 10)
        cancellable.cancel()
    }

    func testLinkingObjectsWillChange() {
        let exp = XCTestExpectation(description: "sink will receive objects")

        let kvoBackLink = BackLink()
        let kvoLinked = Linked()

        kvoBackLink.list.append(kvoLinked)
        realm.add(kvoBackLink)
        try! realm.commitWrite()

        let cancellable = kvoLinked.backlink.objectWillChange.sink { (_: LinkingObjects<BackLink>) in
            //            XCTAssertEqual(kvoLinked.backlink, $0)
            exp.fulfill()
        }

        try! realm.write { kvoBackLink.list.append(Linked()) }

        wait(for: [exp], timeout: 10)
        cancellable.cancel()
    }
 */
}

#endif // canImport(Combine)
