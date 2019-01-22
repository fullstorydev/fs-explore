//Command fs-explore is an entry point for running various utilities/analysis on
//data export files.
//
//Usage: ./fs-explore <utility name> [<utility arguments>...]
package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "fs-explore needs at least one argument (the name of the utility to use)\n")
		fmt.Fprintf(os.Stderr, "Usage: ./fs-explore <utility name> [<utility arguments>...]\n")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "bysession":
		if err := runBysession(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, err.Error() + "\n")
		}
	default:
		fmt.Fprintf(os.Stderr,"Unknown utility name passed to fs-explore\n")
	}
}
