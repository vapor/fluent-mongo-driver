import FluentKit
import BSON

#if compiler(<6)
extension ObjectId: RandomGeneratable {
    public static func generateRandom() -> ObjectId {
        .init()
    }
}
#else
extension ObjectId: @retroactive RandomGeneratable {
    public static func generateRandom() -> ObjectId {
        .init()
    }
}
#endif
