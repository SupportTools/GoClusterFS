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

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
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
	metadata  *FileMetadata
	isLocked  bool
	lockOwner uint64
}

var fileTable = map[string]*FileMetadata{}
var currentBlock int64 = 1 // Start from block 1 to reserve block 0 for metadata

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

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.Println("Attr called for File")
	attr.Inode = f.metadata.Inode
	attr.Mode = 0666
	attr.Size = f.metadata.Size
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	buf := make([]byte, blockSize)
	_, err := device.ReadAt(buf, f.metadata.Block*blockSize)
	if err != nil {
		return nil, err
	}
	return buf[:f.metadata.Size], nil
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	switch req.Name {
	case "user.lock":
		owner, _ := strconv.ParseUint(string(req.Xattr), 10, 64)
		return f.SetLock(ctx, owner)
	case "user.unlock":
		owner, _ := strconv.ParseUint(string(req.Xattr), 10, 64)
		return f.ReleaseLock(ctx, owner)
	}
	return syscall.ENODATA
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if uint64(len(req.Data)) > blockSize {
		return fuse.Errno(syscall.EFBIG)
	}
	_, err := device.WriteAt(req.Data, f.metadata.Block*blockSize)
	if err != nil {
		return fuse.Errno(syscall.EIO)
	}
	f.metadata.Size = uint64(len(req.Data))
	resp.Size = len(req.Data)

	if err := saveMetadata(); err != nil {
		log.Printf("Failed to save metadata: %v", err)
	}
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.Println("Flush called for File")
	return device.Sync() // Sync the device to flush all buffered data to disk
}

type Dir struct{}

func (Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.Println("Attr called for Dir")
	attr.Inode = 1
	attr.Mode = os.ModeDir | 0555
	attr.Uid = uint32(os.Getuid())
	attr.Gid = uint32(os.Getgid())
	return nil
}

func (d Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Println("ReadDirAll called for Dir")
	var entries []fuse.Dirent
	for _, meta := range fileTable {
		entries = append(entries, fuse.Dirent{Inode: meta.Inode, Name: meta.Filename, Type: fuse.DT_File})
	}
	return entries, nil
}

func (d Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Printf("Lookup called for name: %s", name)
	if meta, ok := fileTable[name]; ok {
		return &File{metadata: meta}, nil
	}
	return nil, fuse.ENOENT // No such file or directory
}

func (d Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.Printf("Create called for %s", req.Name)
	meta := &FileMetadata{
		Inode:    uint64(currentBlock + 3),
		Filename: req.Name,
		Size:     0,
		Block:    currentBlock,
	}
	fileTable[req.Name] = meta
	currentBlock++

	f := &File{metadata: meta}
	if err := saveMetadata(); err != nil {
		log.Printf("Failed to save metadata: %v", err)
	}
	return f, f, nil
}

type FS struct{}

func (FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	totalBlocks := deviceSize / blockSize
	freeBlocks := totalBlocks - int64(len(fileTable))

	resp.Blocks = uint64(totalBlocks)
	resp.Bfree = uint64(freeBlocks)
	resp.Bavail = uint64(freeBlocks)
	resp.Files = uint64(len(fileTable))
	resp.Ffree = uint64(freeBlocks)
	resp.Bsize = blockSize
	resp.Namelen = 255
	resp.Frsize = blockSize

	return nil
}

func (FS) Root() (fs.Node, error) {
	log.Println("Root called")
	return Dir{}, nil
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

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("GoClusterFS"),
		fuse.Subtype("goclusterfs"),
		fuse.AllowOther(),
		fuse.MaxReadahead(1<<32-1),
		fuse.AsyncRead(),
		fuse.WritebackCache(),
	)

	if err != nil {
		log.Println("Error mounting:", err)
		os.Exit(1)
	}
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Println("Failed to serve filesystem:", err)
		os.Exit(1)
	}
}
