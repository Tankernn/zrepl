package endpoint

import (
	"context"
	"fmt"
	"regexp"

	"github.com/pkg/errors"

	"github.com/zrepl/zrepl/zfs"
)

var stepHoldTagRE = regexp.MustCompile("^zrepl_STEP_J_(.+)")

func StepHoldTag(jobid JobID) (string, error) {
	t := fmt.Sprintf("zrepl_STEP_J_%s", jobid.String())
	if err := zfs.ValidHoldTag(t); err != nil {
		return "", err
	}
	return t, nil
}

// err != nil always means that the bookmark is not a step bookmark
func ParseStepHoldTag(tag string) (JobID, error) {
	match := stepHoldTagRE.FindStringSubmatch(tag)
	if match == nil {
		return JobID{}, fmt.Errorf("parse hold tag: match regex %q", stepHoldTagRE)
	}
	jobID, err := MakeJobID(match[1])
	if err != nil {
		return JobID{}, errors.Wrap(err, "parse hold tag: invalid job id field")
	}
	return jobID, nil
}

// v must be validated by caller
func StepBookmarkName(fs string, guid uint64, id JobID) (string, error) {
	bmname := fmt.Sprintf("zrepl_STEP_G_%016x_J_%s", guid, id.String())
	if err := zfs.EntityNamecheck(fmt.Sprintf("%s#%s", fs, bmname), zfs.EntityTypeBookmark); err != nil {
		return "", err
	}
	return bmname, nil
}

// name is the full bookmark name, including dataset path
//
// err != nil always means that the bookmark is not a step bookmark
func ParseStepBookmarkName(name string) (guid uint64, jobID JobID, err error) {
	if err := zfs.EntityNamecheck(name, zfs.EntityTypeBookmark); err != nil {
		return 0, JobID{}, errors.Wrap(err, "parse step bookmark")
	}
	var jobIDStr string
	_, err = fmt.Sscanf(name, "zrepl_STEP_G_%016x_J_%s", &guid, &jobIDStr)
	if err != nil {
		return 0, JobID{}, errors.Wrap(err, "step boomark name does not match format string")
	}
	if len(jobIDStr) == 0 {
		return 0, JobID{}, errors.New("parse step bookmark: empty job id field")
	}
	jobID, err = MakeJobID(jobIDStr)
	if err != nil {
		return 0, JobID{}, errors.Wrap(err, "parse step bookmark: invalid job id")
	}
	return guid, jobID, nil
}

const ReplicationCursorBookmarkName = "zrepl_replication_cursor"

// may return nil for both values, indicating there is no cursor
func GetReplicationCursor(fs *zfs.DatasetPath) (*zfs.FilesystemVersion, error) {
	versions, err := zfs.ZFSListFilesystemVersions(fs, nil) // FIXME use ZFSGet on precomputed bookmark name?
	if err != nil {
		return nil, err
	}
	for _, v := range versions {
		if v.Type == zfs.Bookmark && v.Name == ReplicationCursorBookmarkName {
			return &v, nil
		}
	}
	return nil, nil
}

// `target` is validated before replication cursor is set. if validation fails, the cursor is not moved.
//
// returns ErrBookmarkCloningNotSupported if version is a bookmark and bookmarking bookmarks is not supported by ZFS
func SetReplicationCursor(ctx context.Context, fs string, target *zfs.ZFSSendArgVersion) (err error) {
	if len(fs) == 0 {
		return errors.New("filesystem name must not be empty")
	}

	snapProps, err := target.ValidateExistsAndGetCheckedProps(ctx, fs)
	if err != nil {
		return errors.Wrapf(err, "invalid replication cursor target %q (guid=%v)", target.RelName, target.GUID)
	}

	bookmarkPath := fmt.Sprintf("%s#%s", fs, ReplicationCursorBookmarkName)
	bookmarkProps, err := zfs.ZFSGetCreateTXGAndGuid(bookmarkPath)
	_, bookmarkNotExistErr := err.(*zfs.DatasetDoesNotExist)
	if err != nil && !bookmarkNotExistErr {
		return errors.Wrap(err, "cannot get bookmark txg")
	}
	if err == nil {
		// bookmark does exist

		if snapProps.CreateTXG < bookmarkProps.CreateTXG {
			return errors.New("can only be advanced, not set back")
		}

		if bookmarkProps.Guid == snapProps.Guid {
			return nil // no action required
		}

		// FIXME make safer by using new temporary bookmark, then rename, possible with channel programs
		// https://github.com/zfsonlinux/zfs/pull/7902/files might support this but is too new
		if err := zfs.ZFSDestroy(bookmarkPath); err != nil {
			return errors.Wrap(err, "cannot destroy current cursor to move it to new")
		}
		// fallthrough
	}

	if err := zfs.ZFSBookmark(fs, *target, ReplicationCursorBookmarkName); err != nil {
		if err == zfs.ErrBookmarkCloningNotSupported {
			return err // TODO go1.13 use wrapping
		}
		return errors.Wrapf(err, "cannot create bookmark")
	}

	return nil
}

// idempotently hold / step-bookmark `version`
//
// returns ErrBookmarkCloningNotSupported if version is a bookmark and bookmarking bookmarks is not supported by ZFS
func HoldStep(ctx context.Context, fs string, v *zfs.ZFSSendArgVersion, jobID JobID) error {
	if err := v.ValidateExists(ctx, fs); err != nil {
		return err
	}
	if v.IsSnapshot() {

		tag, err := StepHoldTag(jobID)
		if err != nil {
			return errors.Wrap(err, "step hold tag")
		}

		if err := zfs.ZFSHold(ctx, fs, *v, tag); err != nil {
			return errors.Wrap(err, "step hold: zfs")
		}

		return nil
	}

	v.MustBeBookmark()

	bmname, err := StepBookmarkName(fs, v.GUID, jobID)
	if err != nil {
		return errors.Wrap(err, "create step bookmark: determine bookmark name")
	}
	// idempotently create bookmark
	err = zfs.ZFSBookmark(fs, *v, bmname)
	if err != nil {
		if err == zfs.ErrBookmarkCloningNotSupported {
			// TODO we could actually try to find a local snapshot that has the requested GUID
			// 		however, the replication algorithm prefers snapshots anyways, so this quest
			// 		is most likely not going to be successful. Also, there's the possibility that
			//      the caller might want to filter what snapshots are eligibile, and this would
			//      complicate things even further.
			return err // TODO go1.13 use wrapping
		}
		return errors.Wrap(err, "create step bookmark: zfs")
	}
	return nil
}

func ReleaseStep(ctx context.Context, fs string, v *zfs.ZFSSendArgVersion, jobID JobID) error {
	if err := v.ValidateExists(ctx, fs); err != nil {
		return err
	}

	if v.IsSnapshot() {
		tag, err := StepHoldTag(jobID)
		if err != nil {
			return errors.Wrap(err, "step release tag")
		}

		if err := zfs.ZFSRelease(ctx, tag, v.FullPath(fs)); err != nil {
			return errors.Wrap(err, "step release: zfs")
		}

		return nil
	}

	v.MustBeBookmark()

	bmname, err := StepBookmarkName(fs, v.GUID, jobID)
	if err != nil {
		return errors.Wrap(err, "step release: determine bookmark name")
	}
	// idempotently destroy bookmark

	if err := zfs.ZFSDestroyIdempotent(bmname); err != nil {
		return errors.Wrap(err, "step release: bookmark destroy: zfs")
	}

	return nil
}

// release {step holds, step bookmarks} earlier and including `mostRecent`
func ReleaseStepAll(ctx context.Context, fs string, mostRecent *zfs.ZFSSendArgVersion, jobID JobID) error {

	fsp, err := zfs.NewDatasetPath(fs)
	if err != nil {
		return errors.Wrap(err, "step release all: invalid filesystem path")
	}

	mostRecentProps, err := mostRecent.ValidateExistsAndGetCheckedProps(ctx, fs)
	if err != nil {
		return errors.Wrap(err, "step release all: validate most recent version argument")
	}

	tag, err := StepHoldTag(jobID)
	if err != nil {
		return errors.Wrap(err, "step release all: tag")
	}

	err = zfs.ZFSReleaseAllOlderAndIncludingGUID(ctx, fs, mostRecent.GUID, tag)
	if err != nil {
		return errors.Wrapf(err, "step release all: release holds older and including %q", mostRecent.FullPath(fs))
	}

	stepBookmarks, err := zfs.ZFSListFilesystemVersions(fsp, zfs.FilterFromClosure(
		func(t zfs.VersionType, name string) (accept bool, err error) {

			_, parsedId, parseErr := ParseStepBookmarkName(fs + "#" + name)
			return t == zfs.Bookmark && parseErr == nil && parsedId == jobID, nil
		}))
	if err != nil {
		return errors.Wrap(err, "step release all: list step bookmarks")
	}

	// cut off all bookmarks up to and including mostRecent.CreateTXG
	var destroy []zfs.FilesystemVersion
	for _, v := range stepBookmarks {
		_, _, parseErr := ParseStepBookmarkName(fs + "#" + v.Name)
		if parseErr != nil {
			panic("implementation error: filter should guarantee we only have step bookmarks in results")
		}
		if v.CreateTXG < mostRecentProps.CreateTXG {
			destroy = append(destroy, v)
		}
	}
	// FIXME use batch destroy, must adopt code to handle bookmarks
	for _, v := range destroy {
		if err := zfs.ZFSDestroyIdempotent(v.ToAbsPath(fsp)); err != nil {
			return errors.Wrap(err, "step release all: destroy step bookmark")
		}
	}

	return nil
}

type StepBookmark struct {
	FS, Name string
	Guid     uint64
	JobID    JobID
}
type StepHold struct {
	FS, Snap string
	JobID    JobID
}

type ListStepAllOutput struct {
	StepBookmarks []*StepBookmark
	StepHolds     []*StepHold
}

func ListStepAll(ctx context.Context) (*ListStepAllOutput, error) {
	out := &ListStepAllOutput{
		StepBookmarks: make([]*StepBookmark, 0),
		StepHolds:     make([]*StepHold, 0),
	}
	if err := doListStepAll(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

func doListStepAll(ctx context.Context, out *ListStepAllOutput) error {
	fss, err := zfs.ZFSListMapping(ctx, zfs.NoFilter())
	if err != nil {
		return errors.Wrap(err, "list filesystems")
	}

	for _, fs := range fss {
		fsvs, err := zfs.ZFSListFilesystemVersions(fs, nil)
		if err != nil {
			return errors.Wrapf(err, "list filesystem versions of %q", fs)
		}
		for _, v := range fsvs {
			fullname := v.ToAbsPath(fs)
			if v.Type == zfs.Bookmark {
				sbm := &StepBookmark{FS: fs.ToString(), Name: v.Name}
				sbm.Guid, sbm.JobID, err = ParseStepBookmarkName(fullname)
				if err != nil {
					continue
				}
				out.StepBookmarks = append(out.StepBookmarks, sbm)
			} else if v.Type == zfs.Snapshot {

				holds, err := zfs.ZFSHolds(ctx, fs.ToString(), v.Name)
				if err != nil {
					return errors.Wrapf(err, "get holds of %q", fullname)
				}

				for _, hold := range holds {
					jobID, err := ParseStepHoldTag(hold)
					if err != nil {
						continue
					}
					hold := &StepHold{FS: fs.ToString(), Snap: v.Name, JobID: jobID}
					out.StepHolds = append(out.StepHolds, hold)
				}
			} else {
				panic("unknown version")
			}
		}
	}
	return nil
}

var lastReceivedHoldTagRE = regexp.MustCompile("^zrepl_last_received_J_(.+)$")

// err != nil always means that the bookmark is not a step bookmark
func ParseLastReceivedHoldTag(tag string) (JobID, error) {
	match := lastReceivedHoldTagRE.FindStringSubmatch(tag)
	if match == nil {
		return JobID{}, errors.Errorf("parse last-received-hold tag: does not match regex %s", lastReceivedHoldTagRE.String())
	}
	jobId, err := MakeJobID(match[1])
	if err != nil {
		return JobID{}, errors.Wrap(err, "parse last-received-hold tag: invalid job id field")
	}
	return jobId, nil
}

func LastReceivedHoldTag(jobID JobID) (string, error) {
	tag := fmt.Sprintf("zrepl_last_received_J_%s", jobID.String())
	if err := zfs.ValidHoldTag(tag); err != nil {
		return "", err
	}
	return tag, nil
}

func MoveLastReceivedHold(ctx context.Context, fs string, to zfs.ZFSSendArgVersion, jobID JobID) error {
	if err := to.ValidateExists(ctx, fs); err != nil {
		return err
	}
	if err := zfs.EntityNamecheck(to.FullPath(fs), zfs.EntityTypeSnapshot); err != nil {
		return err
	}

	tag, err := LastReceivedHoldTag(jobID)
	if err != nil {
		return errors.Wrap(err, "last-received-hold: hold tag")
	}

	// we never want to be without a hold
	// => hold new one before releasing old hold

	err = zfs.ZFSHold(ctx, fs, to, tag)
	if err != nil {
		return errors.Wrap(err, "last-received-hold: hold newly received")
	}

	err = zfs.ZFSReleaseAllOlderThanGUID(ctx, fs, to.GUID, tag)
	if err != nil {
		return errors.Wrap(err, "last-received-hold: release older holds")
	}

	return nil
}

type LastReceivedHold struct {
	FS    string
	Snap  string
	Tag   string
	JobID JobID
}

func ListLastReceivedAll(ctx context.Context) ([]LastReceivedHold, error) {
	fss, err := zfs.ZFSListMapping(ctx, zfs.NoFilter())
	if err != nil {
		return nil, errors.Wrap(err, "list filesystems")
	}

	out := make([]LastReceivedHold, 0)
	for _, fs := range fss {
		fsvs, err := zfs.ZFSListFilesystemVersions(fs, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "list filesystem versions of %q", fs)
		}
		for _, v := range fsvs {
			if v.Type != zfs.Snapshot {
				continue
			}
			fullPath := v.ToAbsPath(fs)
			holds, err := zfs.ZFSHolds(ctx, fs.ToString(), v.Name)
			if err != nil {
				return nil, errors.Wrapf(err, "get holds of %q", fullPath)
			}
			for _, tag := range holds {
				jobID, err := ParseLastReceivedHoldTag(tag)
				if err != nil {
					continue // not the tag we're looking for
				}
				out = append(out, LastReceivedHold{
					FS:    fs.ToString(),
					JobID: jobID,
					Snap:  v.Name,
					Tag:   tag,
				})
			}
		}
	}
	return out, nil
}
