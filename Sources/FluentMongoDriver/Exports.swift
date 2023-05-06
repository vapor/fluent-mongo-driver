#if swift(>=5.8)

@_documentation(visibility: internal) @_exported import struct BSON.ObjectId
@_documentation(visibility: internal) @_exported import struct MongoKitten.GridFSFile
@_documentation(visibility: internal) @_exported import struct BSON.Document
@_documentation(visibility: internal) @_exported import protocol BSON.Primitive

#else

@_exported import struct BSON.ObjectId
@_exported import struct MongoKitten.GridFSFile
@_exported import struct BSON.Document
@_exported import protocol BSON.Primitive

#endif
