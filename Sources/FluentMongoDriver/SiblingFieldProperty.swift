import FluentKit

extension Fields {
    public typealias SiblingField<To> = SiblingFieldProperty<Self, To>
        where To: Model
}

@propertyWrapper
public final class SiblingFieldProperty<From, To>
    where From: Fields, To: Model
{
    public let key: FieldKey
    public var value: To?
    public var identifier: To.IDValue?
    
    public var projectedValue: SiblingFieldProperty<From, To> { self }

    public var wrappedValue: To? {
        get {
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
        guard let identifier = self.identifier else {
            fatalError("Cannot query siblings relation from unsaved model.")
        }

        return To.query(on: database)
            .filter(\._$id == identifier)
    }
}

extension SiblingFieldProperty: Property {
    public typealias Model = From
    public typealias Value = To
}

extension SiblingFieldProperty: Relation {
    public var name: String {
        return "SiblingsField<\(From.self), \(To.self)>(key: \(self.key))"
    }

    public func load(on database: Database) -> EventLoopFuture<Void> {
        self.query(on: database).first().map {
            self.value = $0
        }
    }
}

extension SiblingFieldProperty: EagerLoadable where From: FluentKit.Model {
    public static func eagerLoad<Builder>(
        _ relationKey: KeyPath<From, From.SiblingField<To>>,
        to builder: Builder
    )
        where Builder: EagerLoadBuilder, Builder.Model == From
    {
        let loader = SiblingEagerLoader(relationKey: relationKey)
        builder.add(loader: loader)
    }


    public static func eagerLoad<Loader, Builder>(
        _ loader: Loader,
        through: KeyPath<From, From.SiblingField<To>>,
        to builder: Builder
    ) where
        Loader: EagerLoader,
        Loader.Model == To,
        Builder: EagerLoadBuilder,
        Builder.Model == From
    {
        let loader = ThroughSiblingEagerLoader(relationKey: through, loader: loader)
        builder.add(loader: loader)
    }
}

extension SiblingFieldProperty: AnyProperty {
    public var nested: [AnyProperty] { [] }
    public var path: [FieldKey] { [self.key] }

    public func input(to input: inout DatabaseInput) {
        input.set(identifier.map { identifiers in
            .bind(identifiers)
        } ?? .null, at: key)
    }

    public func output(from output: DatabaseOutput) throws {
        if output.contains(key) {
            self.identifier = nil
            
            do {
                self.identifier = try output.decode(self.key, as: Optional<To.IDValue>.self)
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
        if let identifier = self.identifier {
            var container = encoder.singleValueContainer()
            try container.encode(identifier)
        }
    }

    public func decode(from decoder: Decoder) throws {
        self.identifier = try To.IDValue(from: decoder)
    }
}

private struct SiblingEagerLoader<From, To>: EagerLoader
    where From: Model, To: Model
{
    let relationKey: KeyPath<From, SiblingFieldProperty<From, To>>

    func run(models: [From], on database: Database) -> EventLoopFuture<Void> {
        let done = models.map { model -> EventLoopFuture<Void> in
            guard let id = model[keyPath: self.relationKey].identifier else {
                model[keyPath: self.relationKey].value = nil
                return database.eventLoop.makeSucceededFuture(())
            }
            
            return To.query(on: database).filter(\._$id == id).first().map { result in
                model[keyPath: self.relationKey].value = result
            }
        }
        
        return EventLoopFuture.andAllSucceed(done, on: database.eventLoop)
    }
}

private struct ThroughSiblingEagerLoader<From, Through, Loader>: EagerLoader
    where From: Model, Loader: EagerLoader, Loader.Model == Through
{
    let relationKey: KeyPath<From, From.SiblingField<Through>>
    let loader: Loader

    func run(models: [From], on database: Database) -> EventLoopFuture<Void> {
        let throughs = models.compactMap { model in
            model[keyPath: self.relationKey].value
        }
        return self.loader.run(models: throughs, on: database)
    }
}
