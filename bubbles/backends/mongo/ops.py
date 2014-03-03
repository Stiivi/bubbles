# -*- coding: utf-8 -*-
from .objects import MongoDBCollection

from ...metadata import *
from ...errors import *
from ...prototypes import *
from ...objects import *

def prepare_mongo_key(key):
    key = prepare_key(key)
    return {name:1 for name in key}


#############################################################################
# Metadata Operations

@field_filter.register("mongo")
def _(ctx, obj, keep=None, drop=None, rename=None, filter=None):

    if rename:
        raise NotImplementedError("Renaming of MongoDB fields is not "
                                    "implemented")

    if filter:
        if keep or drop or rename:
            raise OperationError("Either filter or keep, drop, rename should "
                                 "be used")
        else:
            field_filter = filter
    else:
        field_filter = FieldFilter(keep=keep, drop=drop, rename=rename)

    new_fields = field_filter.filter(obj.fields)

    # No need to actually do anything just pretend that we have new fields.

    return obj.clone(fields=new_fields)

#############################################################################
# Row Operations


@distinct.register("mongo")
def _(ctx, obj, key=None):

    if not key:
        key = obj.fields.names()

    key = prepare_mongo_key(key)

    new_fields = obj.fields.fields(key)
    cursor = obj.collection.group(key, {}, {}, "function(obj, prev){}")
    return IterableRecordsDataSource(cursor, new_fields)

