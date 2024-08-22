package utils

import (
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

type PasswordPrompter interface {
	Run() (string, error)
}

type defaultPasswordPrompter struct {
	inputLabelText string
	stdin          *os.File
	stdout         *os.File
}

var _ PasswordPrompter = (*defaultPasswordPrompter)(nil)

func (pp *defaultPasswordPrompter) Run() (string, error) {
	fmt.Fprint(pp.stdout, pp.inputLabelText, " ")
	password, err := term.ReadPassword(int(pp.stdin.Fd()))
	if err != nil {
		return "", err
	}
	fmt.Fprintln(pp.stdout)

	return string(password), nil
}

func NewDefaultPasswordPrompter(inputLabelText string, stdin *os.File, stdout *os.File) (*defaultPasswordPrompter, error) {
	if stdin == nil {
		return nil, fmt.Errorf("stdin cannot be nil")
	}

	if stdout == nil {
		return nil, fmt.Errorf("stdout cannot be nil")
	}

	inputLabelText = strings.TrimSpace(inputLabelText)
	if inputLabelText == "" {
		return nil, fmt.Errorf("input label text cannot be empty")
	}

	return &defaultPasswordPrompter{
		inputLabelText: inputLabelText,
		stdin:          stdin,
		stdout:         stdout,
	}, nil
}
