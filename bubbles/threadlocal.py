# -*- Encoding: utf8 -*-
import threading

thread_locals = threading.local()

# Modified version of LocalProxy from werkzeug by mitsuhiko

class LocalProxy(object):
    """Acts as a proxy for a local.  Forwards all operations to a proxied
    object.  The only operations not supported for forwarding are right handed
    operands and any kind of assignment.
    """

    def __init__(self, name, factory=None):
        object.__setattr__(self, '_local_name', name)
        object.__setattr__(self, '_locals', thread_locals)
        object.__setattr__(self, '_proxy_factory', factory)

    def _represented_local_object(self):
        """Return the represented thread local object. If object does not
        exist, try to create it using factory.
        """
        try:
            return getattr(self._locals, self._local_name)
        except AttributeError:
            if self._proxy_factory:
                setattr(self._locals, self._local_name, self._proxy_factory())
                return getattr(self._locals, self._local_name)
            else:
                raise RuntimeError("No thread local '%s' and no factory "
                                    "provided" % self._local_name)

    @property
    def __idict__(self):
        try:
            return self._represented_local_object().__dict__
        except RuntimeError:
            raise AttributeError('__dict__')

    def __repr__(self):
        try:
            obj = self._represented_local_object()
        except RuntimeError:
            return '<%s unbound>' % self.__class__.__name__
        return repr(obj)

    def __bool__(self):
        try:
            return bool(self._represented_local_object())
        except RuntimeError:
            return False

    def __unicode__(self):
        try:
            return unicode(self._represented_local_object())
        except RuntimeError:
            return repr(self)

    def __dir__(self):
        try:
            return dir(self._represented_local_object())
        except RuntimeError:
            return []

    def __getattr__(self, name):
        if name == '__members__':
            return dir(self._represented_local_object())
        return getattr(self._represented_local_object(), name)

    def __setitem__(self, key, value):
        self._represented_local_object()[key] = value

    def __delitem__(self, key):
        del self._represented_local_object()[key]

    __setattr__ = lambda x, n, v: setattr(x._represented_local_object(), n, v)
    __delattr__ = lambda x, n: delattr(x._represented_local_object(), n)
    __str__ = lambda x: str(x._represented_local_object())
    __lt__ = lambda x, o: x._represented_local_object() < o
    __le__ = lambda x, o: x._represented_local_object() <= o
    __eq__ = lambda x, o: x._represented_local_object() == o
    __ne__ = lambda x, o: x._represented_local_object() != o
    __gt__ = lambda x, o: x._represented_local_object() > o
    __ge__ = lambda x, o: x._represented_local_object() >= o
    __cmp__ = lambda x, o: cmp(x._represented_local_object(), o)
    __hash__ = lambda x: hash(x._represented_local_object())
    __call__ = lambda x, *a, **kw: x._represented_local_object()(*a, **kw)
    __len__ = lambda x: len(x._represented_local_object())
    __getitem__ = lambda x, i: x._represented_local_object()[i]
    __iter__ = lambda x: iter(x._represented_local_object())
    __contains__ = lambda x, i: i in x._represented_local_object()
    __add__ = lambda x, o: x._represented_local_object() + o
    __sub__ = lambda x, o: x._represented_local_object() - o
    __mul__ = lambda x, o: x._represented_local_object() * o
    __floordiv__ = lambda x, o: x._represented_local_object() // o
    __mod__ = lambda x, o: x._represented_local_object() % o
    __divmod__ = lambda x, o: x._represented_local_object().__divmod__(o)
    __pow__ = lambda x, o: x._represented_local_object() ** o
    __lshift__ = lambda x, o: x._represented_local_object() << o
    __rshift__ = lambda x, o: x._represented_local_object() >> o
    __and__ = lambda x, o: x._represented_local_object() & o
    __xor__ = lambda x, o: x._represented_local_object() ^ o
    __or__ = lambda x, o: x._represented_local_object() | o
    __div__ = lambda x, o: x._represented_local_object().__div__(o)
    __truediv__ = lambda x, o: x._represented_local_object().__truediv__(o)
    __neg__ = lambda x: -(x._represented_local_object())
    __pos__ = lambda x: +(x._represented_local_object())
    __abs__ = lambda x: abs(x._represented_local_object())
    __invert__ = lambda x: ~(x._represented_local_object())
    __complex__ = lambda x: complex(x._represented_local_object())
    __int__ = lambda x: int(x._represented_local_object())
    __long__ = lambda x: long(x._represented_local_object())
    __float__ = lambda x: float(x._represented_local_object())
    __oct__ = lambda x: oct(x._represented_local_object())
    __hex__ = lambda x: hex(x._represented_local_object())
    __index__ = lambda x: x._represented_local_object().__index__()
    __coerce__ = lambda x, o: x._represented_local_object().__coerce__(x, o)
    __enter__ = lambda x: x._represented_local_object().__enter__()
    __exit__ = lambda x, *a, **kw: x._represented_local_object().__exit__(*a, **kw)
    __radd__ = lambda x, o: o + x._represented_local_object()
    __rsub__ = lambda x, o: o - x._represented_local_object()
    __rmul__ = lambda x, o: o * x._represented_local_object()
    __rdiv__ = lambda x, o: o / x._represented_local_object()
    __rtruediv__ = __rdiv__
    __rfloordiv__ = lambda x, o: o // x._represented_local_object()
    __rmod__ = lambda x, o: o % x._represented_local_object()
    __rdivmod__ = lambda x, o: x._represented_local_object().__rdivmod__(o)

