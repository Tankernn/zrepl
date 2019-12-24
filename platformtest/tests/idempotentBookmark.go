package tests

import (
	"fmt"

	"github.com/zrepl/zrepl/platformtest"
	"github.com/zrepl/zrepl/zfs"
)

func IdempotentBookmark(ctx *platformtest.Context) {

	platformtest.Run(ctx, platformtest.PanicErr, ctx.RootDataset, `
		DESTROYROOT
		CREATEROOT
		+  "foo bar"
		+  "foo bar@a snap"
		+  "foo bar@another snap"
	`)

	fs := fmt.Sprintf("%s/foo bar", ctx.RootDataset)

	asnap := sendArgVersion(fs, "@a snap")
	anotherSnap := sendArgVersion(fs, "@another snap")

	err := zfs.ZFSBookmark(fs, asnap, "a bookmark")
	if err != nil {
		panic(err)
	}

	// do it again, should be idempotent
	err = zfs.ZFSBookmark(fs, asnap, "a bookmark")
	if err != nil {
		panic(err)
	}

	// should fail for another snapshot
	err = zfs.ZFSBookmark(fs, anotherSnap, "a bookmark")
	if err == nil {
		panic(err)
	}
	if _, ok := err.(*zfs.BookmarkExists); !ok {
		panic(fmt.Sprintf("has type %T", err))
	}

	// destroy the snapshot
	if err := zfs.ZFSDestroy(fmt.Sprintf("%s@a snap", fs)); err != nil {
		panic(err)
	}

	// do it again, should fail with special error type
	err = zfs.ZFSBookmark(fs, asnap, "a bookmark")
	if err == nil {
		panic(err)
	}
	if _, ok := err.(*zfs.DatasetDoesNotExist); !ok {
		panic(fmt.Sprintf("has type %T", err))
	}

}
