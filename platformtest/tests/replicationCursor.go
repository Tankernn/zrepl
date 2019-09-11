package tests

import (
	"fmt"

	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/platformtest"
	"github.com/zrepl/zrepl/zfs"
)

func ReplicationCursor(ctx *platformtest.Context) {

	platformtest.Run(ctx, platformtest.PanicErr, ctx.RootDataset, `
		CREATEROOT
		+  "foo bar"
		+  "foo bar@1 with space"
	`)

	ds, err := zfs.NewDatasetPath(ctx.RootDataset + "/foo bar")
	if err != nil {
		panic(err)
	}

	fs := ds.ToString()
	snap := sendArgVersion(fs, "@1 with space")

	err = endpoint.SetReplicationCursor(ctx, fs, &snap)
	if err != nil {
		panic(err)
	}

	snapProps, err := zfs.ZFSGetCreateTXGAndGuid(snap.FullPath(fs))
	if err != nil {
		panic(err)
	}

	bm, err := endpoint.GetReplicationCursor(ds)
	if err != nil {
		panic(err)
	}
	if bm.CreateTXG != snapProps.CreateTXG {
		panic(fmt.Sprintf("createtxgs do not match: %v != %v", bm.CreateTXG, snapProps.CreateTXG))
	}
	if bm.Guid != snapProps.Guid {
		panic(fmt.Sprintf("guids do not match: %v != %v", bm.Guid, snapProps.Guid))
	}

	// test nonexistent
	err = zfs.ZFSDestroyFilesystemVersion(ds, bm)
	if err != nil {
		panic(err)
	}
	bm2, err := endpoint.GetReplicationCursor(ds)
	if bm2 != nil {
		panic(fmt.Sprintf("expecting no replication cursor after deleting it, got %v", bm))
	}
	if err != nil {
		panic(fmt.Sprintf("expecting no error for getting nonexistent replication cursor, bot %v", err))
	}

	// TODO test moving the replication cursor
}
