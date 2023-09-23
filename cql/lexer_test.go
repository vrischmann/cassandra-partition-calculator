package cql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type tokens []token

func (t *tokens) pop() token {
	tmp := (*t)[0]
	*t = (*t)[1:]

	return tmp
}

func collectTokens(t testing.TB, lexer *lexer) tokens {
	res := make([]token, 0, 128)

loop:
	for {
		token, err := lexer.Next()
		switch {
		case errors.Is(err, errEOF):
			break loop
		case err != nil:
			require.NoError(t, err)
		}

		res = append(res, token)
	}

	return res
}

func TestLexer(t *testing.T) {
	testCases := []struct {
		input string
		exp   []string
	}{
		{
			input: `CREATE TABLE events(
				tenant_key bigint,
				user_id uuid,
				event_category text,
				event_id timeuuid,
				event_data blob,
				PRIMARY KEY ((tenant_key, user_id, event_category), event_id)
			);`,
			exp: []string{
				"CREATE", "TABLE", "events",
				"(",
				"tenant_key", "bigint", ",",
				"user_id", "uuid", ",",
				"event_category", "text", ",",
				"event_id", "timeuuid", ",",
				"event_data", "blob", ",",
				"PRIMARY", "KEY", "(",
				"(", "tenant_key", ",", "user_id", ",", "event_category", ")",
				",", "event_id", ")",
				")",
				";",
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid PRIMARY KEY,
				event_data blob
			);`,
			exp: []string{
				"CREATE", "TABLE", "events",
				"(",
				"user_id", "uuid", "PRIMARY", "KEY", ",",
				"event_data", "blob",
				")",
				";",
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid PRIMARY KEY,
				event_data text STATIC
			);`,
			exp: []string{
				"CREATE", "TABLE", "events",
				"(",
				"user_id", "uuid", "PRIMARY", "KEY", ",",
				"event_data", "text", "STATIC",
				")",
				";",
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid PRIMARY KEY,
				event_data map<int, text>,
				weights set<double>
			);`,
			exp: []string{
				"CREATE", "TABLE", "events",
				"(",
				"user_id", "uuid", "PRIMARY", "KEY", ",",
				"event_data", "map", "<", "int", ",", "text", ">", ",",
				"weights", "set", "<", "double", ">",
				")",
				";",
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			lexer := newLexer(tc.input)

			tokens := collectTokens(t, lexer)
			require.Len(t, tokens, len(tc.exp))

			for _, exp := range tc.exp {
				require.Equal(t, token(exp), tokens.pop())
			}
		})
	}
}

func TestLexerUndo(t *testing.T) {
	nextEq := func(t testing.TB, lexer *lexer, s string) {
		token, err := lexer.Next()
		require.NoError(t, err)
		require.Equal(t, s, token.String())
	}

	t.Run("single", func(t *testing.T) {
		lexer := newLexer("user_id uuid")

		nextEq(t, lexer, "user_id")
		nextEq(t, lexer, "uuid")

		lexer.Undo()

		nextEq(t, lexer, "uuid")

		lexer.Undo()
		lexer.Undo()
		lexer.Undo()
		lexer.Undo()

		nextEq(t, lexer, "user_id")
		nextEq(t, lexer, "uuid")
	})

	t.Run("all", func(t *testing.T) {
		lexer := newLexer("user_id uuid")

		lexer.ResetUndo()

		nextEq(t, lexer, "user_id")
		nextEq(t, lexer, "uuid")

		lexer.UndoAll()

		nextEq(t, lexer, "user_id")
		nextEq(t, lexer, "uuid")

		lexer.UndoAll()
		lexer.UndoAll()

		nextEq(t, lexer, "user_id")
		nextEq(t, lexer, "uuid")
	})
}

func TestLexerAllWhitespace(t *testing.T) {
	lexer := newLexer("user_id      ")

	token, err := lexer.Next()
	require.NoError(t, err)
	require.Equal(t, "user_id", token.String())

	_, err = lexer.Next()
	require.ErrorIs(t, err, errEOF)
}
