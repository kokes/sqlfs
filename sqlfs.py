import sqlite3
import os
import math
import codecs


class fs:
    def __init__(self, path, max_buffer_size=8192, max_sql_row_size=4096):
        init = path == ':memory:' or not os.path.isfile(path)
        self.conn = sqlite3.connect(path)
        self.path = path
        # TODO: this should be bufsize like in open (1 -> line buffered, 0 -> unbuffered)
        self.max_buffer_size = max_buffer_size
        self.max_sql_row_size = max_sql_row_size

        if init:
            self._initial_cte()

    def __enter__(self):
        return self

    def __del__(self):
        self.conn.close()

    def __exit__(self, type, value, traceback):
        self.conn.close()

    def close(self):
        self.conn.close()

    def _initial_cte(self):
        # create table if not exist? not ideal, if the existing table is not of the same structure
        self.conn.execute(
            'create table files(id integer primary key autoincrement, name varchar, is_directory bool default false, offset integer, contents_length integer, contents blob);'
        )

    def open(self, path, mode='r'):
        return _FileHandler(self, path, mode)


class _FileHandler:
    def __init__(self, fs, path, mode='r', encoding='utf8'):
        if mode not in ['r', 'rt', 'w', 'wt', 'rb', 'wb', 'r+', 'a+', 'a']:
            raise NotImplementedError('mode %s not implemented' % mode)

        self.fs = fs
        self._text_mode = mode in ['r', 'w', 'rt', 'wt', 'r+', 'a', 'a+']
        self._read_mode = mode in ['r', 'rt', 'rb', 'r+']
        self._write_mode = mode in ['w', 'wb', 'wt', 'r+', 'a', 'a+']
        self._seek_mode = mode in ['r', 'rt', 'rb', 'r+', 'w', 'wb',
                                   'wt']  # TODO: complete this
        self.mode = mode

        if not (self._read_mode or self._write_mode):
            raise ValueError('mode %s not properly setup' % mode)

        self.path = path

        self.encoding = codecs.lookup(encoding)
        self._buffersize = 0
        self._buffer = []
        self._position = 0

        # file exists?
        lb = self.fs.conn.execute(
            'select offset+contents_length from files where name = ? order by offset desc limit 1',
            (self.path, )).fetchone()
        file_exists = lb is not None

        # in certain modes, files have to exist
        if not file_exists and mode in ['a', 'r', 'rt', 'rb']:
            raise OSError('file does not exist')

        # seeking to the end to append
        if file_exists and mode in ['a', 'a+']:
            self._position = lb[0]

        # adding an empty file
        if not file_exists and self._write_mode:
            self._touch()

        # truncating
        if file_exists and mode in ['w', 'wb', 'wt']:
            self.fs.conn.execute('delete from files where name = ?',
                                 (self.path, ))
            self.fs.conn.commit()

            self._touch()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass  # it could have been closed already

    def _touch(self):
        self.write('' if self._text_mode else b'')

    def _addtobuffer(self, data):
        # append bytes either way
        bdata = data.encode() if isinstance(data, str) else data
        self._buffer.append(bdata)
        self._buffersize += len(bdata)

        if self._buffersize > self.fs.max_buffer_size:
            self.flush()

    def seek(self, offset, from_what=0):
        if not self._seek_mode:
            raise IOError('cannot seek in mode %s' % self.mode)
        # TODO: https://stackoverflow.com/questions/21533391/seeking-from-end-of-file-throwing-unsupported-exception
        if from_what not in [0, 1, 2]:
            raise ValueError('whence can only be 0, 1 or 2')

        if from_what != 0:
            raise NotImplementedError

        self.flush()

        self._position = offset

        return self._position

    def tell(self):
        self.flush()
        return self._position

    def __next__(self):
        for el in self:
            return el

    def readline(self):
        return next(self)

    def readlines(self):
        return list(self)

    def __iter__(self):
        ex = self.fs.conn.execute(
            'select offset, contents from files where name = ? and (offset + contents_length) > ? order by offset asc',
            (self.path, self._position))

        remaining = b''
        for row in ex:
            ind = -1
            pind = self._position - row[0]
            while True:
                pind += ind + 1
                ind = row[1][pind:].find(b'\n')
                if ind == -1:
                    remaining += row[1][pind:]
                    break
                else:
                    stub = remaining + row[1][pind:pind + ind + 1]
                    self._position += len(stub)
                    if self._text_mode:
                        # yield self.encoding.decode(stub)[0]
                        yield stub.decode(self.encoding.name)
                    else:
                        yield stub

        if len(remaining) > 0:
            self._position += len(remaining)
            if self._text_mode:
                # yield self.encoding.decode(remaining)[0]
                yield remaining.decode(self.encoding.name)
            else:
                yield remaining

    def read(self, size=None):
        # TODO: will break on utf8 in some cases (when we're "in the middle" of a two-byte character)
        assert size is None or size >= 0  # TODO (f.read(-1) should read all, f.read(-n) should read nothing)

        ex = self.fs.conn.execute(
            'select offset, contents from files where name = ? and (offset + contents_length) >= ? order by offset asc',
            (self.path, self._position))

        res = []
        fo, fr = next(ex)
        res.append(fr[(self._position - fo):])
        for row in ex:
            res.append(row[1])

        stub = b''.join(res)
        # TODO: inefficient - if size <> None, only load as much data as needed
        stub = stub[:size] if not self._text_mode else stub.decode(
            self.encoding.name)[:size]

        # so how many bytes did we read in the end?
        self._position += len(stub) if isinstance(stub, bytes) else len(
            stub.encode())

        return stub

    def write(self, data):
        if not (isinstance(data, str) or isinstance(data, bytes)):
            raise TypeError('can only write bytes or strings')

        if self._text_mode and not isinstance(data, str):
            raise TypeError(
                'can only write strings to a file opened in text mode')

        if not self._text_mode and not isinstance(data, bytes):
            raise TypeError(
                'can only write bytes to a file opened in binary mode')

        if not self._write_mode:
            raise ValueError('cannot write to a read-only file')

        self._addtobuffer(data)

        return len(data)

    def flush(self):
        # anything to do?
        if len(self._buffer) == 0:
            return

        lastwrittenbyte = self.fs.conn.execute(
            'select offset+contents_length from files where name = ? order by offset desc limit 1',
            (self.path, )).fetchone()

        newfile = lastwrittenbyte is None

        if newfile:
            offset = 0
        else:
            existing = self.fs.conn.execute(
                '''select
                id, offset, contents_length from files
                where name = ?
                and
                    (? >= offset and (offset + contents_length) >= ?)
                order by offset asc
                limit 1''',
                (self.path, self._position, self._position)).fetchone()

            if existing is None:
                raise ValueError('can this happen?')  # TODO
            else:
                rid, offset, contents_length = existing
                total_length = offset + contents_length

                # are we appending
                if total_length == self._position:
                    if contents_length < self.fs.max_sql_row_size:
                        # fairly wasteful
                        exc = self.fs.conn.execute(
                            'select contents from files where id = ?',
                            (rid, )).fetchone()[0]
                        self._buffer.insert(0, exc)
                        self._buffersize += len(exc)
                        # TODO: defer this until after the write? Otherwise we might lose data mid-write
                        self.fs.conn.execute('delete from files where id = ?',
                                             (rid, ))
                    else:
                        # start a brand new row
                        offset = total_length
                # or filling in nulls
                elif total_length < self._position:
                    # illegal offset should result in NULL bytes (f=open('a', 'w'); f.seek(5); f.write('abc') -> b'\x00\x00\x00\x00\x00abc')
                    # so we'll just insert this byte array into our buffer
                    # but it's otherwise the same as appending
                    # bytes(n) -> b'\x00'*n
                    pad = bytes(self._position - total_length)
                    self._buffer.insert(0, pad)
                    self._buffersize += len(pad)
                # or rewriting data
                elif total_length > self._position:
                    # where does the affected datastream end?
                    endp = self._position + self._buffersize

                    # number of existing chunks + last byte written
                    nchunks, endw, mino, maxo = self.fs.conn.execute(
                        'select count(*), max(offset+contents_length), min(offset), max(offset) from files where name = ?',
                        (self.path, )).fetchone()

                    # a single chunk OR the write will go beyond all existing data
                    if nchunks == 1 or (nchunks > 1 and endp > endw):
                        exc = self.fs.conn.execute(
                            'select contents from files where id = ?',
                            (rid, )).fetchone()[0]

                        start = exc[:self._position - offset]
                        end = exc[(
                            self._position + self._buffersize - offset):]
                        self._buffer.insert(0, start)
                        self._buffer.append(end)
                        self._buffersize += len(start) + len(end)
                        # TODO: defer this until after the write? Otherwise we might lose data mid-write
                        if nchunks == 1:
                            self.fs.conn.execute(
                                'delete from files where id = ?', (rid, ))
                        else:
                            self.fs.conn.execute(
                                'delete from files where name = ?',
                                (self.path, ))  # all shall burn

                    else:
                        # TODO (!!!)
                        raise ValueError('tricky rewriting cases')
                        # take some data from the first and the last stripe
                        # also, be careful about offsets if MAX_SQL_ROW_LENGTH is different
                        # from the length of any of these rows (and thus could mess up
                        # offsets down the line)
                        # test: f.seek(0); cn=f.read(); f.seek(5); f.write(cn[:-20])
                        pass

        roll = 0
        prevend = 0
        leftover = b''
        ins = []
        for j, el in enumerate(self._buffer):
            roll += len(el)
            if roll > self.fs.max_sql_row_size or j == (len(self._buffer) - 1):
                towrite = leftover + b''.join(self._buffer[prevend:j + 1])

                assert len(towrite) == roll  # TODO: remove

                chunks = math.ceil(roll / self.fs.max_sql_row_size)
                # write something if it's the last one
                if chunks == 0 and j == (len(self._buffer) - 1):
                    chunks = 1

                assert chunks > 0  # TODO: remove

                for ch in range(chunks):
                    dt = towrite[ch * self.fs.max_sql_row_size:(
                        ch + 1) * self.fs.max_sql_row_size]

                    ins.append((self.path, offset, len(dt), dt))
                    offset += len(dt)

                leftover = towrite[-(roll % self.fs.max_sql_row_size):]

                roll = len(leftover)
                prevend = j + 1

        self.fs.conn.executemany(
            'insert into files(name, offset, contents_length, contents) values(?, ?, ?, ?)',
            ins)

        self.fs.conn.commit()

        self._buffer = []
        self._buffersize = 0

    def close(self):
        self.fs.conn.commit()
        self.flush()
