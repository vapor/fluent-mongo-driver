import FluentKit
import BSON

extension ObjectId: RandomGeneratable {
    public static func generateRandom() -> ObjectId {
        .init()
    }
}
