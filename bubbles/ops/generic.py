from ..operation import operation
from ..dev import experimental
from ..prototypes import *

#############################################################################
# Convenience

@experimental
@rename_fields.register("*")
def rename_fields(ctx, obj, rename):
    return ctx.op.field_filter(obj, rename=rename)

@experimental
@drop_fields.register("*")
def drop_fields(ctx, obj, drop):
    return ctx.op.field_filter(obj, drop=drop)

@experimental
@keep_fields.register("*")
def keep_fields(ctx, obj, keep):
    return ctx.op.field_filter(obj, keep=keep)

#############################################################################
# Debug

@debug_fields.register("*")
def debug_fields(ctx, obj, label=None):
    if label:
        label = " (%s)" % label
    else:
        label = ""

    ctx.logger.info("fields%s: %s" % (label, obj.fields))
    return obj

