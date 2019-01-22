## FS-EXPLORE

`fs-explore` is a command-line tool for launching small FullStory utilities.

Usage: `./fs-explore <utility name> [<utility arguments>... ]`

#### BYSESSION

`bysession` is a FullStory utility that can be launched by `fs-explore`. It is a tool that can group export files (such as those created by Hauser) by session.

Usage: `./fs-explore bysession <Exports folder>`

Example: `./fs-explore bysession /tmp/myexports`

The example above will look for a folder called `/tmp/myexports`, then write grouped-by-session data into a newly-created `myexports-bysession` folder in the current directory.
