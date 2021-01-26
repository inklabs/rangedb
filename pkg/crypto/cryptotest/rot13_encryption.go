package cryptotest

import (
	"strings"
)

type rotCipher struct{}

func NewRot13Cipher() *rotCipher {
	return &rotCipher{}
}

func (r rotCipher) Encrypt(_, data string) (string, error) {
	return strings.Map(rot, data), nil
}

func (r rotCipher) Decrypt(_, cipherText string) (string, error) {
	return strings.Map(rot, cipherText), nil
}

func rot(r rune) rune {
	capital := r >= 'A' && r <= 'Z'
	if !capital && (r < 'a' || r > 'z') {
		return r
	}

	r += 13
	if capital && r > 'Z' || !capital && r > 'z' {
		r -= 26
	}
	return r
}
