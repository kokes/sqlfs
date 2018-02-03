import unittest
import tempfile
import os

import sqlfs

# Helper classes


class InitTdir:
    def setUp(self):
        self.tdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tdir.cleanup()


class InitFS(InitTdir):
    def setUp(self):
        super(InitFS, self).setUp()
        self.fs = sqlfs.fs(os.path.join(self.tdir.name, 'fs.db'))

    def tearDown(self):
        super(InitFS, self).tearDown()
        self.fs.close()


# Actual tests


class TestInitFS(InitTdir, unittest.TestCase):
    def test_createfs(self):
        with sqlfs.fs(os.path.join(self.tdir.name, 'fs.db')) as fs:
            pass


class TestIteration(InitFS, unittest.TestCase):
    def setUp(self):
        super(TestIteration, self).setUp()

        content = b'abc\ndef\nghi'

        self.fn = 'foobar.txt'
        with self.fs.open(self.fn, 'wb') as f:
            f.write(content)

    def test_next_iter(self):
        with self.fs.open(self.fn, 'r') as f:
            self.assertEqual('abc\n', next(f))
            # self.assertEqual(b'def\n', next(f))
            # self.assertEqual(b'ghi\n', next(f))

        with self.fs.open(self.fn, 'rb') as f:
            self.assertEqual(b'abc\n', next(f))
            # self.assertEqual(b'def\n', next(f))
            # self.assertEqual(b'ghi\n', next(f))

    def test_readline(self):
        with self.fs.open(self.fn, 'r') as f:
            self.assertEqual('abc\n', f.readline())
            # self.assertEqual(b'def\n', f.readline())
            # self.assertEqual(b'ghi\n', f.readline())

        with self.fs.open(self.fn, 'rb') as f:
            self.assertEqual(b'abc\n', f.readline())
            # self.assertEqual(b'def\n', f.readline())
            # self.assertEqual(b'ghi\n', f.readline())

    def test_readlines(self):
        with self.fs.open(self.fn, 'r') as f:
            self.assertEqual(['abc\n', 'def\n', 'ghi'], f.readlines())

        with self.fs.open(self.fn, 'r') as f:
            lr = list(f)
            f.seek(0)
            self.assertEqual(lr, f.readlines())

        with self.fs.open(self.fn, 'rb') as f:
            self.assertEqual([b'abc\n', b'def\n', b'ghi'], f.readlines())

        with self.fs.open(self.fn, 'rb') as f:
            lr = list(f)
            f.seek(0)
            self.assertEqual(lr, f.readlines())


class TestRead(InitFS, unittest.TestCase):
    # TODO: test utf8 character read ('ěšč'.read(3) -> 'ěšč')
    def setUp(self):
        super(TestRead, self).setUp()

        self.content = 'The name is Ondřej'

        self.fn = 'foobar.txt'
        with self.fs.open(self.fn, 'w') as f:
            f.write(self.content)

    def test_read_all(self):
        with self.fs.open(self.fn, 'rt') as f1, self.fs.open(
                self.fn, 'r') as f2, self.fs.open(self.fn) as f3:
            self.assertEqual(f1.read(), self.content)
            self.assertEqual(f2.read(), self.content)
            self.assertEqual(f3.read(), self.content)

        with self.fs.open(self.fn, 'rb') as f:
            self.assertEqual(f.read(), self.content.encode())

    def test_read_some(self):
        with self.fs.open(self.fn, 'rt') as f1, self.fs.open(
                self.fn, 'r') as f2, self.fs.open(self.fn) as f3:
            self.assertEqual(f1.read(3), self.content[:3])
            self.assertEqual(f2.read(3), self.content[:3])
            self.assertEqual(f3.read(3), self.content[:3])

            self.assertEqual(f1.read(0), '')
            self.assertEqual(f2.read(0), '')
            self.assertEqual(f3.read(0), '')

        # with self.fs.open(self.fn, 'rb') as f:
        #     self.assertEqual(f.read(), self.content.encode())


# TODO: also test appends (around boundaries especially) and illegal seeks
class TestRewrite(InitFS, unittest.TestCase):
    def test_rewrite(self):
        self.fn = 'foobar.txt'
        with self.fs.open(self.fn, 'w') as f:
            f.write('abcdef')
            f.seek(1)
            f.write('ghi')

        with self.fs.open(self.fn, 'rt') as f:
            self.assertEqual(f.read(), 'aghief')

        with self.fs.open(self.fn, 'w') as f:
            f.write('abcdef')
            f.seek(0)
            f.write('ghijkl')

        with self.fs.open(self.fn, 'rt') as f:
            self.assertEqual(f.read(), 'ghijkl')

        with self.fs.open(self.fn, 'w') as f:
            f.write('abcdef')
            f.seek(1)
            f.write('ghijklmnop')

        with self.fs.open(self.fn, 'rt') as f:
            self.assertEqual(f.read(), 'aghijklmnop')


class TestOverflownRewrite(InitFS, unittest.TestCase):
    def test_rewrite(self):
        self.fn = 'foobar.txt'
        sqlfs.max_sql_row_size = 12
        content = 'foobar' * sqlfs.max_sql_row_size

        with self.fs.open(self.fn, 'w') as f:
            f.write(content)

        with self.fs.open(self.fn, 'r+') as f:
            f.seek(5)
            f.write('bazbak' * sqlfs.max_sql_row_size)

        # TODO: doesn't test anything
        # perhaps lower max_sql_row_size in our filesystem
        # with self.fs.open(self.fn, 'rt') as f:
        #     self.assertEqual(f.read(), 'aghijklmnop')


if __name__ == '__main__':
    unittest.main()