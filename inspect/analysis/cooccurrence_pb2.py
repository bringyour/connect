# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: cooccurrence.proto
# Protobuf Python Version: 5.28.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    28,
    2,
    '',
    'cooccurrence.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x12\x63ooccurrence.proto\x12\tbringyour\")\n\tCoocInner\x12\x0b\n\x03sid\x18\x01 \x01(\t\x12\x0f\n\x07overlap\x18\x02 \x01(\x04\"B\n\tCoocOuter\x12\x0b\n\x03sid\x18\x01 \x01(\t\x12(\n\ncooc_inner\x18\x02 \x03(\x0b\x32\x14.bringyour.CoocInner\"<\n\x10\x43ooccurrenceData\x12(\n\ncooc_outer\x18\x01 \x03(\x0b\x32\x14.bringyour.CoocOuterB\x18Z\x16\x62ringyour.com/protocolb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'cooccurrence_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  _globals['DESCRIPTOR']._loaded_options = None
  _globals['DESCRIPTOR']._serialized_options = b'Z\026bringyour.com/protocol'
  _globals['_COOCINNER']._serialized_start=33
  _globals['_COOCINNER']._serialized_end=74
  _globals['_COOCOUTER']._serialized_start=76
  _globals['_COOCOUTER']._serialized_end=142
  _globals['_COOCCURRENCEDATA']._serialized_start=144
  _globals['_COOCCURRENCEDATA']._serialized_end=204
# @@protoc_insertion_point(module_scope)
