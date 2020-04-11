////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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
import Combine
import Realm.Private

// MARK: - Protocols

public protocol ObjectKeyIdentifable: Identifiable, Object {
    var id: UInt64 { get }
}

extension ObjectKeyIdentifable {
    public var id: UInt64 {
        RLMObjectBaseGetCombineId(self)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public protocol RealmSubscribable {
    func observe<S>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool) -> NotificationToken where S: Subscriber, S.Input == Self
}

// MARK: - Object

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Object: Combine.ObservableObject {
    public var objectWillChange: RealmPublisher<Object> {
        return RealmPublisher(self)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Object: RealmSubscribable {
    public func observe<S: Subscriber>(on queue: DispatchQueue?, _ subscriber: S, freeze: Bool)
        -> NotificationToken where S.Input: Object {
            return observe(on: queue) { (change: ObjectChange<S.Input>) in
                switch change {
                case .change(let object, _):
                    _ = subscriber.receive(freeze ? object.freeze() : object)
                case .deleted:
                    subscriber.receive(completion: .finished)
                default:
                    break
                }
            }
    }
}

// MARK: - List

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension List: ObservableObject {
    public var objectWillChange: RealmPublisher<List> {
        RealmPublisher(self)
    }
}

// MARK: - LinkingObjects

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension LinkingObjects {
    public var objectWillChange: RealmPublisher<LinkingObjects> {
        RealmPublisher(self)
    }
}

// MARK: - Results

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Results {
    public var objectWillChange: RealmPublisher<Results> {
        RealmPublisher(self)
    }
}

// MARK: RealmCollection

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension RealmCollection {
    public func observe<S>(on queue: DispatchQueue? = nil, _ subscriber: S, freeze: Bool)
        -> NotificationToken where S: Subscriber, S.Input == Self {
            // FIXME: we could skip some pointless work in converting the changeset to the Swift type here
            return observe(on: queue) { change in
                switch change {
                case .update(let collection, deletions: _, insertions: _, modifications: _):
                    _ = subscriber.receive(freeze ? collection.freeze() : collection)
                // FIXME: error
                default:
                    break
                }
            }
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Results: RealmSubscribable {
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension List: RealmSubscribable {
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension LinkingObjects: RealmSubscribable {
}

// MARK: Subscriptions

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmSubscription: Subscription {
    private var token: NotificationToken

    public var combineIdentifier: CombineIdentifier {
        return CombineIdentifier(token)
    }

    init(token: NotificationToken) {
        self.token = token
    }

    public func request(_ demand: Subscribers.Demand) {
    }

    public func cancel() {
        token.invalidate()
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmPublisher<Collection: RealmSubscribable>: Publisher where Collection: ThreadConfined {
    public typealias Failure = Never
    public typealias Output = Collection

    let object: Collection
    let freeze: Bool
    let queue: DispatchQueue?
    public init(_ object: Collection, freeze: Bool = false, queue: DispatchQueue? = nil) {
        self.object = object
        self.freeze = freeze
        self.queue = queue
    }

    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Never, Output == S.Input {
        subscriber.receive(subscription: RealmSubscription(token: self.object.observe(on: queue, subscriber, freeze: freeze)))
    }

    public func subscribe<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil) -> RealmPublisher<Collection> {
        guard let queue = scheduler as? DispatchQueue else {
            fatalError("Cannot subscribe on scheduler \(scheduler): only serial dispatch queues are currently implemented.")
        }
        return RealmPublisher(object, freeze: freeze, queue: queue)
    }

    public func receive<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil) -> RealmHandoverPublisher<Self, S> {
        if freeze {
            return RealmHandoverPublisher(self, scheduler)
        }
        return RealmHandoverPublisher(self, scheduler, self.object.realm!)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct RealmHandoverPublisher<Upstream: Publisher, S: Scheduler>: Publisher where Upstream.Output: ThreadConfined {
    public typealias Failure = Upstream.Failure
    public typealias Output = Upstream.Output

    private let config: Realm.Configuration?
    private let upstream: Upstream
    private let scheduler: S

    internal init(_ upstream: Upstream, _ scheduler: S, _ realm: Realm? = nil) {
        self.config = realm?.configuration
        self.upstream = upstream
        self.scheduler = scheduler
    }

    public func receive<Sub>(subscriber: Sub) where Sub: Subscriber, Sub.Failure == Failure, Output == Sub.Input {
        guard let config = config else {
            return self.upstream.receive(on: scheduler).receive(subscriber: subscriber)
        }
        let scheduler = self.scheduler
        self.upstream
            .map { ThreadSafeReference(to: $0) }
            .receive(on: scheduler)
            .compactMap { $0.resolve(in: try! Realm(configuration: config,
                                                    queue: scheduler as? DispatchQueue)) }
            .receive(subscriber: subscriber)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct CollectionChangePublisher<Collection: RealmCollection>: Publisher {
    public typealias Output = RealmCollectionChange<Collection>
    public typealias Failure = Never

    let collection: Collection
    let freeze: Bool
    let queue: DispatchQueue?
    public init(_ collection: Collection, freeze: Bool = false, queue: DispatchQueue? = nil) {
        self.collection = collection
        self.freeze = freeze
        self.queue = queue
    }

    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Never, Output == S.Input {
        let token = self.collection.observe(on: self.queue) { change in
            if !self.freeze {
                _ = subscriber.receive(change)
                return
            }

            let frozen: Output
            switch change {
            case .initial(let c):
                frozen = .initial(c.freeze())
                break
            case .update(let c, deletions: let deletions, insertions: let insertions, modifications: let modifications):
                frozen = .update(c.freeze(), deletions: deletions, insertions: insertions, modifications: modifications)
            case .error:
                frozen = change
                break
            }
            _ = subscriber.receive(frozen)
        }
        subscriber.receive(subscription: RealmSubscription(token: token))
    }

    public func subscribe<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil) -> CollectionChangePublisher<Collection> {
        guard let queue = scheduler as? DispatchQueue else {
            fatalError("Cannot subscribe on scheduler \(scheduler): only serial dispatch queues are currently implemented.")
        }
        return CollectionChangePublisher(collection, freeze: freeze, queue: queue)
    }

//    public func receive<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil)
//        -> RealmHandoverPublisher<Self, S> where Collection: ThreadConfined {
//            if freeze {
//                return RealmHandoverPublisher(self, scheduler)
//            }
//            return RealmHandoverPublisher(self, scheduler, self.object.realm!)
//    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct ObjectChangePublisher<T: Object>: Publisher {
    public typealias Output = ObjectChange<T>
    public typealias Failure = Never

    let object: T
    let freeze: Bool
    let queue: DispatchQueue?
    public init(_ object: T, freeze: Bool = false, queue: DispatchQueue? = nil) {
        self.object = object
        self.freeze = freeze
        self.queue = queue
    }

    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Never, Output == S.Input {
        let token = self.object.observe(on: self.queue) { change in
            switch change {
            case .change(let o, let properties):
                _ = subscriber.receive(.change((self.freeze ? o.freeze() : o) as! T, properties))
            case .error(let error):
                _ = subscriber.receive(.error(error))
                break
            case .deleted:
                subscriber.receive(completion: .finished)
                return
            }
        }
        subscriber.receive(subscription: RealmSubscription(token: token))
    }

    public func subscribe<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil) -> ObjectChangePublisher<T> {
        guard let queue = scheduler as? DispatchQueue else {
            fatalError("Cannot subscribe on scheduler \(scheduler): only serial dispatch queues are currently implemented.")
        }
        return ObjectChangePublisher(object, freeze: freeze, queue: queue)
    }

//    public func receive<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil)
//        -> RealmHandoverPublisher<Self, S> where Collection: ThreadConfined {
//            if freeze {
//                return RealmHandoverPublisher(self, scheduler)
//            }
//            return RealmHandoverPublisher(self, scheduler, self.object.realm!)
//    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
extension Publisher where Output: ThreadConfined {
    public func makeThreadSafe() -> MakeThreadSafePublisher<Self> {
        MakeThreadSafePublisher(self)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct MakeThreadSafePublisher<Upstream: Publisher>: Publisher where Upstream.Output: ThreadConfined {
    public typealias Failure = Upstream.Failure
    public typealias Output = Upstream.Output

    let upstream: Upstream
    public init(_ upstream: Upstream) {
        self.upstream = upstream
    }

    public func receive<S>(subscriber: S) where S: Subscriber, S.Failure == Failure, Output == S.Input {
        self.upstream.receive(subscriber: subscriber)
    }

    public func receive<S: Scheduler>(on scheduler: S, options: S.SchedulerOptions? = nil) -> DeferredHandoverPublisher<Upstream, S> {
        DeferredHandoverPublisher(self.upstream, scheduler)
    }
}

@available(OSX 10.15, watchOS 6.0, iOS 13.0, iOSApplicationExtension 13.0, OSXApplicationExtension 10.15, *)
public struct DeferredHandoverPublisher<Upstream: Publisher, S: Scheduler>: Publisher where Upstream.Output: ThreadConfined {
    public typealias Failure = Upstream.Failure
    public typealias Output = Upstream.Output

    private let upstream: Upstream
    private let scheduler: S

    internal init(_ upstream: Upstream, _ scheduler: S) {
        self.upstream = upstream
        self.scheduler = scheduler
    }

    private enum Handover {
        case object(_ object: Output)
        case tsr(_ tsr: ThreadSafeReference<Output>, config: Realm.Configuration)
    }

    public func receive<Sub>(subscriber: Sub) where Sub: Subscriber, Sub.Failure == Failure, Output == Sub.Input {
        // FIXME: needs to bypass RealmSwift Configuration for accessor caching to work
        let scheduler = self.scheduler
        self.upstream
            .map { (obj: Output) -> Handover in
                guard let realm = obj.realm, !realm.isFrozen else { return .object(obj) }
                return .tsr(ThreadSafeReference(to: obj), config: realm.configuration)
            }
            .receive(on: scheduler)
            .compactMap { (handover: Handover) -> Output? in
                switch handover {
                case .object(let obj):
                    return obj
                case .tsr(let tsr, let config):
                    return tsr.resolve(in: try! Realm(configuration: config,
                                                      queue: scheduler as? DispatchQueue))
                }
            }
            .receive(subscriber: subscriber)
    }
}
#endif // canImport(Combine)
