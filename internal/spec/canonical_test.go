package spec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalJSON(t *testing.T) {
	in := map[string]any{
		"b":   1,
		"a":   []any{true, nil, "x"},
		"€":   "euro sorts after ASCII",
		"é":   "é (U+00E9) sorts before € (U+20AC)",
		"esc": "quote\" back\\ nl\n tab\t ctl\x01 html<>& u2028",
	}
	out, err := canonicalJSON(in)
	require.NoError(t, err)
	assert.Equal(t,
		`{"a":[true,null,"x"],"b":1,"esc":"quote\" back\\ nl\n tab\t ctl\u0001 html<>& u2028`+""+`",`+
			`"é":"é (U+00E9) sorts before € (U+20AC)","€":"euro sorts after ASCII"}`,
		string(out))
}

func TestCanonicalJSONRejectsFloats(t *testing.T) {
	_, err := canonicalJSON(map[string]any{"x": 1.5})
	assert.ErrorContains(t, err, "only integers are supported")
}

func TestLessUTF16(t *testing.T) {
	// U+1F600 (surrogate pair D83D DE00) sorts before U+FB01 in UTF-16 order,
	// although it is the larger code point.
	assert.True(t, lessUTF16("\U0001F600", "ﬁ"))
	assert.True(t, lessUTF16("a", "ab"))
	assert.False(t, lessUTF16("b", "a"))
}
