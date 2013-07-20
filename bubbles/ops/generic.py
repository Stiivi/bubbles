from ..operation import operation
from ..dev import experimental

#############################################################################
# Convenience

@operation("*")
@experimental
def rename_fields(ctx, obj, rename):
    return ctx.o.field_filter(obj, rename=rename)

@operation("*")
@experimental
def drop_fields(ctx, obj, drop):
    return ctx.o.field_filter(obj, drop=drop)

@operation("*")
@experimental
def keep_fields(ctx, obj, keep):
    return ctx.o.field_filter(obj, keep=keep)

#############################################################################
# Debug

@operation("*")
def debug_fields(ctx, obj, label=None):
    if label:
        label = " (%s)" % label
    else:
        label = ""

    ctx.logger.info("fields%s: %s" % (label, obj.fields))
    return obj

