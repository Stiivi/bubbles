from .errors import *
from .core import default_context

__all__ = [
            "ImmediatePipeline"
        ]

class ImmediatePipeline(object):
    def __init__(self, stores=None, context=None, obj=None):
        """Creates a new pipeline with `context` and sets current object to
        `obj`. If no context is provided, default one is used.

        Pipeline inherits operations from the `context` and uses context's
        dispatcher to call the operations. Operations are provided as
        pipeline's methods:

        .. code-block:: python

            p = Pipeline(stores={"default":source_store})
            p.source("default", "data")
            # Call an operation within context
            p.distinct("city")

            p.create("default", "cities")
        """
        self.context = context or default_context
        self.result = obj
        self.stores = stores or {}

    def source(self, obj, store=None, **kwargs):
        """Sets an object `obj` from store `store` as
        source of the pipeline. If `obj` is a name, then `store` is required,
        if `obj` is actual object, then store should not be specified.
        Pipeline should be empty – no existing resul should be present."""

        if self.result is not None:
            raise BubblesError("Can not set pipeline source: result already "
                                "exists (%s). Use new pipeline." %
                                type(self.result))

        if isinstance(obj, str):
            if isinstance(store, str):
                try:
                    store = self.stores[store]
                except KeyError:
                    raise ArgumentError("Unknown store %s" % store)
            self.result = store.get_object(obj, *args, **kwargs)
        else:
            if store:
                raise ArgumentError("Both actual object and store specified, "
                                    "you can use store only with object "
                                    "name")
            self.result = obj

        return self


    def create(self, obj, store=None, **kwargs):
        """Create new object `obj_name` in store `store_name` using current
        result object's fields and data. If no result exists an exception is
        raised. Result will be newly created target."""

        if self.result is None:
            raise BubblesError("Pipeline has no result for new target object")

        if isinstance(store, str):
            try:
                store = self.stores[store]
            except KeyError:
                raise ArgumentError("Unknown store %s" % store)

        target = store.create(obj, fields=self.result.fields, **kwargs)
        target.append_from(self.result)
        self.result = target

    def append_into(self, store_name, obj_name, *args, **kwargs):
        """Appends data into object `obj_name` in store `store_name`. Result
        object will be the target."""

        if self.result is None:
            raise BubblesError("Pipeline has no result for a target object")
        try:
            store = self.stores[store_name]
        except KeyError:
            raise ArgumentError("Unknown store %s" % store_name)

        target = store.get_object(obj_name, *args, **kwargs)
        target.append_from(self.result)
        self.result = target

    def fork(self):
        """Forks current pipeline. Returns a new pipeline with same actual
        object as the receiver."""
        fork = Pipeline(self.stores, self.context, self.result)
        return fork

    def __getattr__(self, name):
        return _PipelineStep(self, name, self.context)

class _PipelineStep(object):
    def __init__(self, pipeline, opname, obj):
        self.pipeline = pipeline
        self.op = self.pipeline.context.operation(opname)
        self.opname = opname
        self.obj = obj

    def __call__(self, *args, **kwargs):
        self.pipeline.result = self.op(self.pipeline.result, *args, **kwargs)

