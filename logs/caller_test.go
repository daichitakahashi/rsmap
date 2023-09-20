package logs

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"
)

func TestCallers(t *testing.T) {
	t.Parallel()

	var c CallerContext
	c = c.Append("one.go", 10)
	c = c.Append("two.go", 30)

	t.Log(c.String())

	var oneHash, twoHash string
	_, err := fmt.Sscanf(
		c.String(),
		"one.go:10(%8s)->two.go:30(%8s)",
		&oneHash,
		&twoHash,
	)
	assert.NilError(t, err, "%s", err)
	t.Logf(oneHash)
	t.Logf(twoHash)
}
