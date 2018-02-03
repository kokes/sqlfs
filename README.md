Reimplementation of Python's file IO, using SQLite for storage. This is *not* an OS-level implementation like FUSE, this is just a Python library meant to simulate a file system.

### Why?

1. A single database file is very portable, copying many slow files may be a bit problematic when using external or network storage.
3. Using a database will allow for a relatively simple versioning mechanism.
4. Hey, it was fun.

This is not about performance (it's performing fairly poorly at the moment), more about usability and portability.

### Usage

The library itself is meant to be single file, so that it's easy to embed. It's meant to be used just like `open` in core Python.

```
with sqlfs.fs('filesystem.db') as fs:
	with fs.open('foobar.txt', 'w') as fl:
		fl.write('this is my new content')
		fl.seek(0)
		fl.write('and overwriting')
```

### Implementation

It's fairly straightforward, each file is saved into one or more slices (its size is user defined). Say you define a slice to be 1024 bytes, a file of one megabyte will occupy a thousand slices. Each file could have been saved into a single blob in one row, but as the files should be seekable and appendable, I didn't want to rewrite one giant blob when a single byte is changed.