package cql

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var (
	errEOF = errors.New("end of file")
)

type token string

func (t token) String() string {
	return string(t)
}

type lexer struct {
	data string
	pos  int

	undoStack []int

	options struct {
		eatWhitespace bool
	}
}

func newLexer(data string) *lexer {
	l := &lexer{
		data: data,
	}
	l.options.eatWhitespace = true

	return l
}

func (l *lexer) char() byte   { return l.data[l.pos] }
func (l *lexer) view() string { return l.data[l.pos:] }

func (l *lexer) ResetUndo() {
	l.undoStack = l.undoStack[:0]
}

func (l *lexer) UndoAll() {
	if len(l.undoStack) <= 0 {
		return
	}

	l.pos = l.undoStack[0]
	l.undoStack = l.undoStack[:0]
}

func (l *lexer) Undo() {
	if len(l.undoStack) <= 0 {
		return
	}

	pos := l.undoStack[len(l.undoStack)-1]
	l.undoStack = l.undoStack[:len(l.undoStack)-1]

	l.pos = pos
}

func (l *lexer) Next() (token, error) {
	if l.pos >= len(l.data) {
		return "", fmt.Errorf("no more data, err: %w", errEOF)
	}

	if l.options.eatWhitespace {
		if err := l.eatWhitespace(); err != nil {
			return "", err
		}
	}

	l.undoStack = append(l.undoStack, l.pos)

	//

	switch ch := l.char(); ch {
	case '(', ')', '<', '>', ',', ';':
		l.pos++

		return token(ch), nil

	case '\t', '\n', '\r', ' ':
		l.pos++
		return token(ch), nil

	default:
		tmp := l.readUntil(isTerminator)
		if tmp == "" {
			tmp = l.view()
		}

		l.pos += len(tmp)

		return token(tmp), nil
	}
}

func (l *lexer) eatWhitespace() error {
	data := l.view()

	for _, ch := range data {
		if !unicode.IsSpace(ch) {
			return nil
		}
		l.pos++
	}

	return fmt.Errorf("no more whitespace to be found, err: %w", errEOF)
}

func (l *lexer) readUntil(predicate func(rune) bool) string {
	data := l.view()

	pos := strings.IndexFunc(data, predicate)
	if pos == -1 {
		return ""
	}

	return data[:pos]
}

func isTerminator(ch rune) bool {
	if unicode.IsSpace(ch) {
		return true
	}

	if ch == ',' {
		return true
	}

	return ch == '(' || ch == ')' || ch == '<' || ch == '>'
}
