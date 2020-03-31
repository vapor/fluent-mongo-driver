import FluentKit

extension Fields {
    public typealias SiblingsField<To> = SiblingsFieldProperty<Self, To>
        where To: Model
}

@propertyWrapper
public final class SiblingsFieldProperty<From, To>
    where From: Fields, To: Model
{
    public let key: FieldKey
    public var value: [To]?
    public var identifiers: [To.IDValue]?
    
    public var projectedValue: SiblingsFieldProperty<From, To> {
        self
    }

    public var wrappedValue: [To] {
        get {
            guard let value = self.value else {
                fatalError("Cannot access field before it is initialized or fetched: \(self.key)")
            }
            return value
        }
        set {
            self.value = newValue
        }
    }
    
    public init(key: FieldKey) {
        self.key = key
    }

    public func query(on database: Database) -> QueryBuilder<To> {
        guard let identifiers = self.identifiers else {
            fatalError("Cannot query siblings relation from unsaved model.")
        }

        return To.query(on: database)
            .filter(\._$id ~~ identifiers)
    }
}

extension SiblingsFieldProperty: PropertyProtocol {
    public typealias Model = From
    public typealias Value = [To]
    
}

extension SiblingsFieldProperty: FieldProtocol { }
extension SiblingsFieldProperty: AnyField { }

extension SiblingsFieldProperty: Relation {
    public var name: String {
        return "SiblingsField<\(From.self), \(To.self)>(key: \(self.key))"
    }

    public func load(on database: Database) -> EventLoopFuture<Void> {
        self.query(on: database).all().map {
            self.value = $0
        }
    }
}

extension SiblingsFieldProperty: AnyProperty {
    public var nested: [AnyProperty] { [] }
    public var path: [FieldKey] { [self.key] }

    public func input(to input: inout DatabaseInput) {
        input.values[self.key] = identifiers.map { identifiers in
            return .bind(identifiers)
        }
    }

    public func output(from output: DatabaseOutput) throws {
        if output.contains([self.key]) {
            self.identifiers = nil
            
            do {
                self.identifiers = try output.decode(self.key, as: [To.IDValue].self)
            } catch {
                throw FluentError.invalidField(
                    name: self.key.description,
                    valueType: [To.IDValue].self,
                    error: error
                )
            }
        }
    }

    public func encode(to encoder: Encoder) throws {
        if let identifiers = self.identifiers {
            var container = encoder.singleValueContainer()
            try container.encode(identifiers)
        }
    }

    public func decode(from decoder: Decoder) throws {
        self.identifiers = try [To.IDValue](from: decoder)
    }
}

extension SiblingsFieldProperty: EagerLoadable where From: FluentKit.Model {
    public static func eagerLoad<Builder>(
        _ relationKey: KeyPath<From, SiblingsFieldProperty<From, To>>,
        to builder: Builder
    ) where
        Builder : EagerLoadBuilder,
        Builder.Model == From
    {
        builder.add(loader: SiblingsEagerLoader(relationKey: relationKey))
    }
    
    public static func eagerLoad<Loader, Builder>(
        _ loader: Loader,
        through relationKey: KeyPath<From, SiblingsFieldProperty<From, To>>,
        to builder: Builder
    ) where
        Loader : EagerLoader,
        Builder : EagerLoadBuilder,
        Builder.Model == From,
        Loader.Model == To
    {
        builder.add(loader: SiblingsEagerLoader(relationKey: relationKey))
    }
}

private struct SiblingsEagerLoader<From, To>: EagerLoader
    where From: Model, To: Model
{
    let relationKey: KeyPath<From, SiblingsFieldProperty<From, To>>

    func run(models: [From], on database: Database) -> EventLoopFuture<Void> {
        let done = models.map { model -> EventLoopFuture<Void> in
            guard let ids = model[keyPath: self.relationKey].identifiers else {
                model[keyPath: self.relationKey].value = []
                return database.eventLoop.makeSucceededFuture(())
            }
            
            let results = ids.map { To.find($0, on: database) }
            
            return EventLoopFuture.whenAllSucceed(results, on: database.eventLoop).map { models in
                model[keyPath: self.relationKey].value = models.compactMap { $0 }
            }
        }
        
        return EventLoopFuture.andAllSucceed(done, on: database.eventLoop)
    }
}

private struct ThroughSiblingsEagerLoader<From, Through, Loader>: EagerLoader
    where From: Model, Loader: EagerLoader, Loader.Model == Through
{
    let relationKey: KeyPath<From, From.SiblingsField<Through>>
    let loader: Loader

    func run(models: [From], on database: Database) -> EventLoopFuture<Void> {
        let throughs = models.flatMap {
            $0[keyPath: self.relationKey].value!
        }
        return self.loader.run(models: throughs, on: database)
    }
}

extension EagerLoader {
    func anyRun(models: [AnyModel], on database: Database) -> EventLoopFuture<Void> {
        self.run(models: models.map { $0 as! Model }, on: database)
    }
}
