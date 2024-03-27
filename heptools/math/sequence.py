class Josephus:
    def __init__(self, size: int, step: int):
        self._size = size
        self._step = step

    def sequence(self, max: int = ...):
        if max is ...:
            max = self._size
        return [*self._loop(self._size, self._step, max)]

    @staticmethod
    def _loop(size: int, step: int, max: int):
        remain = [*range(size)]
        i = 0
        step -= 1
        while len(remain) > (size - max):
            i = (i + step) % len(remain)
            yield remain.pop(i)
