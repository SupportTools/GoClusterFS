package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

var device *os.File
var deviceSize int64

const blockSize = 1024 * 1024 // 1 MB per block
const metadataBlockIndex = 0  // The block index where metadata is stored

type FileMetadata struct {
	Inode    uint64
	Filename string
	Size     uint64
	Block    int64
}

type File struct {
	fs.Inode
	metadata  *FileMetadata
	isLocked  bool
	lockOwner uint64
}

var fileTable = map[string]*FileMetadata{}
var currentBlock int64 = 1 // Start from block 1 to reserve block 0 for metadata

// Compile-time interface checks
var (
	_ fs.InodeEmbedder  = (*File)(nil)
	_ fs.NodeGetattrer  = (*File)(nil)
	_ fs.NodeReader     = (*File)(nil)
	_ fs.NodeWriter     = (*File)(nil)
	_ fs.NodeFsyncer    = (*File)(nil)
	_ fs.NodeSetxattrer = (*File)(nil)
)

var (
	_ fs.InodeEmbedder = (*Dir)(nil)
	_ fs.NodeGetattrer = (*Dir)(nil)
	_ fs.NodeReaddirer = (*Dir)(nil)
	_ fs.NodeLookuper  = (*Dir)(nil)
	_ fs.NodeCreater   = (*Dir)(nil)
)

var (
	_ fs.InodeEmbedder = (*FS)(nil)
	_ fs.NodeOnAdder   = (*FS)(nil)
	_ fs.NodeStatfser  = (*FS)(nil)
)

func openDevice(path string) error {
	var err error
	device, err = os.OpenFile(path, os.O_RDWR, 0660)
	if err != nil {
		return err
	}

	// Determine device size for Statfs
	deviceSize, err = device.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	// Rewind after getting size
	_, err = device.Seek(0, os.SEEK_SET)
	return err
}

func saveMetadata() error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(fileTable); err != nil {
		return err
	}
	_, err := device.WriteAt(buf.Bytes(), metadataBlockIndex*blockSize)
	return err
}

func loadMetadata() error {
	var buf bytes.Buffer
	metadataBlock := make([]byte, blockSize)
	_, err := device.ReadAt(metadataBlock, metadataBlockIndex*blockSize)
	if err != nil {
		return err
	}
	buf.Write(metadataBlock)
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(&fileTable)
	return err
}

func (f *File) SetLock(ctx context.Context, owner uint64) error {
	if f.isLocked {
		return fmt.Errorf("file is already locked")
	}
	f.isLocked = true
	f.lockOwner = owner
	return nil
}

func (f *File) ReleaseLock(ctx context.Context, owner uint64) error {
	if !f.isLocked || f.lockOwner != owner {
		return fmt.Errorf("file is not locked by this owner")
	}
	f.isLocked = false
	f.lockOwner = 0
	return nil
}

func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Println("Attr called for File")
	out.Ino = f.metadata.Inode
	out.Mode = fuse.S_IFREG | 0666
	out.Size = f.metadata.Size
	return 0
}

func (f *File) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	buf := make([]byte, blockSize)
	_, err := device.ReadAt(buf, f.metadata.Block*blockSize)
	if err != nil {
		return nil, syscall.EIO
	}
	data := buf[:f.metadata.Size]

	// Handle offset
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), 0
	}
	data = data[off:]

	// Limit to requested size
	if len(data) > len(dest) {
		data = data[:len(dest)]
	}

	return fuse.ReadResultData(data), 0
}

func (f *File) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	switch attr {
	case "user.lock":
		owner, _ := strconv.ParseUint(string(data), 10, 64)
		if err := f.SetLock(ctx, owner); err != nil {
			return syscall.EBUSY
		}
		return 0
	case "user.unlock":
		owner, _ := strconv.ParseUint(string(data), 10, 64)
		if err := f.ReleaseLock(ctx, owner); err != nil {
			return syscall.EINVAL
		}
		return 0
	}
	return syscall.ENODATA
}

func (f *File) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	if uint64(len(data)) > blockSize {
		return 0, syscall.EFBIG
	}
	_, err := device.WriteAt(data, f.metadata.Block*blockSize)
	if err != nil {
		return 0, syscall.EIO
	}
	f.metadata.Size = uint64(len(data))

	if err := saveMetadata(); err != nil {
		log.Printf("Failed to save metadata: %v", err)
	}
	return uint32(len(data)), 0
}

func (f *File) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	log.Println("Fsync called for File")
	if err := device.Sync(); err != nil {
		return syscall.EIO
	}
	return 0
}

type Dir struct {
	fs.Inode
}

func (d *Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Println("Attr called for Dir")
	out.Ino = 1
	out.Mode = fuse.S_IFDIR | 0555
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	return 0
}

// dirStream implements fs.DirStream
type dirStream struct {
	entries []fuse.DirEntry
	pos     int
}

func (ds *dirStream) HasNext() bool {
	return ds.pos < len(ds.entries)
}

func (ds *dirStream) Next() (fuse.DirEntry, syscall.Errno) {
	if ds.pos >= len(ds.entries) {
		return fuse.DirEntry{}, 0
	}
	e := ds.entries[ds.pos]
	ds.pos++
	return e, 0
}

func (ds *dirStream) Close() {}

func (d *Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Println("Readdir called for Dir")
	var entries []fuse.DirEntry
	for _, meta := range fileTable {
		entries = append(entries, fuse.DirEntry{
			Ino:  meta.Inode,
			Name: meta.Filename,
			Mode: fuse.S_IFREG,
		})
	}
	return &dirStream{entries: entries}, 0
}

func (d *Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("Lookup called for name: %s", name)
	if meta, ok := fileTable[name]; ok {
		file := &File{metadata: meta}
		child := d.NewPersistentInode(ctx, file, fs.StableAttr{
			Mode: fuse.S_IFREG,
			Ino:  meta.Inode,
		})
		out.Ino = meta.Inode
		out.Mode = fuse.S_IFREG | 0666
		out.Size = meta.Size
		return child, 0
	}
	return nil, syscall.ENOENT
}

func (d *Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	log.Printf("Create called for %s", name)
	meta := &FileMetadata{
		Inode:    uint64(currentBlock + 3),
		Filename: name,
		Size:     0,
		Block:    currentBlock,
	}
	fileTable[name] = meta
	currentBlock++

	file := &File{metadata: meta}
	child := d.NewPersistentInode(ctx, file, fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  meta.Inode,
	})

	if err := saveMetadata(); err != nil {
		log.Printf("Failed to save metadata: %v", err)
	}

	out.Ino = meta.Inode
	out.Mode = fuse.S_IFREG | 0666
	out.Size = 0

	return child, nil, 0, 0
}

type FS struct {
	fs.Inode
}

func (f *FS) OnAdd(ctx context.Context) {
	dir := &Dir{}
	ch := f.NewPersistentInode(ctx, dir, fs.StableAttr{
		Mode: fuse.S_IFDIR,
		Ino:  1,
	})
	f.AddChild("", ch, true)
}

func (f *FS) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	totalBlocks := deviceSize / blockSize
	freeBlocks := totalBlocks - int64(len(fileTable))

	out.Blocks = uint64(totalBlocks)
	out.Bfree = uint64(freeBlocks)
	out.Bavail = uint64(freeBlocks)
	out.Files = uint64(len(fileTable))
	out.Ffree = uint64(freeBlocks)
	out.Bsize = blockSize
	out.NameLen = 255
	out.Frsize = blockSize

	return 0
}

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		log.Println("Usage: <program> MOUNTPOINT BLOCK_DEVICE")
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	blockDevice := flag.Arg(1)

	err := openDevice(blockDevice)
	if err != nil {
		log.Fatalf("Failed to open device: %v", err)
	}
	defer device.Close()

	if err := loadMetadata(); err != nil {
		log.Printf("Failed to load metadata: %v", err)
	}

	root := &FS{}
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:       "GoClusterFS",
			Name:         "goclusterfs",
			AllowOther:   true,
			MaxReadAhead: 1 << 17, // 128KB
		},
	}

	server, err := fs.Mount(mountpoint, root, opts)
	if err != nil {
		log.Println("Error mounting:", err)
		os.Exit(1)
	}

	log.Println("Filesystem mounted. Press Ctrl+C to unmount.")
	server.Wait()
}
