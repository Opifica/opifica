// Command signer will run the Opifica signing service: it signs release
// artifact digests with keys held in an HSM or KMS, so signing keys never
// touch a build machine. Not implemented yet.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "signer: not implemented yet")
	os.Exit(1)
}
