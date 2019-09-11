package client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/zrepl/zrepl/endpoint"

	"github.com/zrepl/zrepl/cli"
)

var (
	HoldsCmd = &cli.Subcommand{
		Use:   "holds",
		Short: "manage holds & step bookmarks",
		SetupSubcommands: func() []*cli.Subcommand {
			return holdsList
		},
	}
)

var holdsList = []*cli.Subcommand{
	&cli.Subcommand{
		Use:             "list",
		Run:             doHoldsList,
		NoRequireConfig: true,
	},
}

func doHoldsList(sc *cli.Subcommand, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("migration does not take arguments, got %v", args)
	}

	ctx := context.Background()

	stepHolds, err := endpoint.ListStepAll(ctx)
	if err != nil {
		return err // context clear by invocation of command
	}

	lastReceivedHolds, err := endpoint.ListLastReceivedAll(ctx)
	if err != nil {
		return err // context clear by invocation of command
	}

	type Listing struct {
		StepHolds         *endpoint.ListStepAllOutput
		LastReceivedHolds []endpoint.LastReceivedHold
	}

	listing := Listing{
		StepHolds:         stepHolds,
		LastReceivedHolds: lastReceivedHolds,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("  ", "  ")
	if err := enc.Encode(listing); err != nil {
		panic(err)
	}

	return nil
}
