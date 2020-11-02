import pickle

import clickhouse_driver.errors as err


def picklable(o):
    picked = pickle.loads(pickle.dumps(o))
    assert repr(o) == repr(picked)
    assert str(o) == str(picked)


def test_exception_picklable():
    picklable(err.Error('foo'))
    picklable(err.Error(message='foo'))

    picklable(err.ServerException('foo', 0, Exception()))
    picklable(err.ServerException(message='foo', code=0, nested=Exception()))
