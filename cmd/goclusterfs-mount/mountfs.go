package main

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/goclusterfs"
)

// mountFS mounts gfs at dir as fs.Mount does, through the raw file system that drops flocks at close.
func mountFS(dir string, gfs *goclusterfs.FS, opts *fs.Options) (*fuse.Server, error) {
	server, err := fuse.NewServer(gfs.RawFS(opts), dir, &opts.MountOptions)
	if err != nil {
		return nil, err
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		return nil, err
	}
	return server, nil
}
