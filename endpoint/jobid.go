package endpoint

import (
	"encoding/json"
	"fmt"

	"github.com/zrepl/zrepl/zfs"
)

// An instance of this type returned by MakeJobID guarantees
// that that instance's JobID.String() can be used in a ZFS dataset name and hold tag.
type JobID struct {
	jid string
}

func MakeJobID(s string) (JobID, error) {
	if len(s) == 0 {
		return JobID{}, fmt.Errorf("must not be empty string")
	}

	_, err := zfs.NewDatasetPath(s)
	if err != nil {
		return JobID{}, fmt.Errorf("must be usable in a ZFS dataset path: %s", err)
	}

	return JobID{s}, nil
}

func MustMakeJobID(s string) JobID {
	jid, err := MakeJobID(s)
	if err != nil {
		panic(err)
	}
	return jid
}

func (j JobID) expectInitialized() {
	if j.jid == "" {
		panic("use of unitialized JobID")
	}
}

func (j JobID) String() string {
	j.expectInitialized()
	return j.jid
}

var _ json.Marshaler = JobID{}
var _ json.Unmarshaler = (*JobID)(nil)

func (j JobID) MarshalJSON() ([]byte, error) { return json.Marshal(j.jid) }

func (j *JobID) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &j.jid)
}

func (j JobID) MustValidate() { j.expectInitialized() }
