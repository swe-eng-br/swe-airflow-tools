# -*- coding: utf-8 -*-

import os

# -*- coding: utf-8 -*-

import os

class IList(object):

    def __init__(self, rows=None, row_init=0):

        self._rows = rows or []
        self._row_init = row_init
        self._current_row = self._row_init

    def __iter__(self):

        # save last row position
        old_current_row = self._current_row

        # try move to first row
        if self.move_first():

            # return de current record
            yield self
            while self.move_next():
                yield self

            # restore old row position, case exists
            self._current_row = old_current_row if old_current_row < self.size else self.size - self._row_init

    def __len__(self):
        return self.size

    @property
    def size(self):
        return len(self._rows)

    @property
    def row(self):
        return self._rows[self._current_row]

    @property
    def rows(self):
        return self._rows

    def load(self, file_path):
        return True

    def move_first(self):
        if self.size > 0:
            self._current_row = self._row_init
            return True
        self._current_row = 0
        return False

    def move_last(self):
        if self.size > 0:
            self._current_row = self.size - self._row_init
            return True
        self._current_row = 0
        return False

    def move_next(self):
        if self.size > ((self._current_row - self._row_init) + 1):
            self._current_row += 1
            return True
        return False

    def move_prev(self):
        if self.size > 0 and (self._current_row - self._row_init) > 1:
            self._current_row -= 1
            return True
        return False


class FileList(IList):

    def __init__(self, file_path=None, row_init=0):

        super(FileList, self).__init__(row_init=row_init)

        if file_path:
            self.load(file_path)

    def load(self, file_path):

        if not os.path.isfile(file_path):
            raise Exception('File not found: {}!'.format(file_path))

        file = open(file_path, 'r')
        self._rows = file.readlines()
        file.close()
        self._file_path = file_path
        self._current_row = 0

        return True