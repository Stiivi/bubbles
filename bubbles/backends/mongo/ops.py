# -*- coding: utf-8 -*-
from .objects import MongoDBCollection

from ...metadata import *
from ...errors import *
from ...operation import operation
from ...objects import *

def prepare_mongo_key(key):
    key = prepare_key(key)
    return {name:1 for name in key}


#############################################################################
# Metadata Operations

@operation("mongo")
def field_filter(ctx, obj, keep=None, drop=None, rename=None, filter=None):

    if filter:
        if keep or drop or rename:
            raise OperationError("Either filter or keep, drop, rename should "
                                 "be used")
        else:
            field_filter = filter
    else:
        field_filter = FieldFilter(keep=keep, drop=drop, rename=rename)

    new_fields = field_filter.filter(obj.fields)

    return obj.clone(fields=new_fields)

#############################################################################
# Row Operations


@operation("mongo")
def distinct(ctx, obj, key=None):

    if not key:
        key = obj.fields.names()

    key = prepare_mongo_key(key)

    new_fields = obj.fields.fields(key)
    cursor = obj.collection.group(key, {}, {}, "function(obj, prev){}")
    return IterableRecordsDataSource(cursor, new_fields)

