package crypto

import "strings"

// HashPassword produces an argon2id verifier for a dashboard password, using
// the same parameters as API key hashing. Passwords are never stored in
// plaintext; only this hash is persisted.
func HashPassword(plaintext string) (string, error) {
	return HashAPIKey(plaintext)
}

// VerifyPassword reports whether plaintext matches the stored hash. It detects
// the hash format: argon2id PHC strings are verified via argon2; bcrypt hashes
// ($2a$/$2b$) are verified via bcrypt.CompareHashAndPassword for backward
// compatibility with 9router/foreign imports. The caller can re-hash a
// bcrypt match by calling HashPassword + persisting the new argon2id hash.
func VerifyPassword(plaintext, encodedHash string) (bool, error) {
	if strings.HasPrefix(encodedHash, "$2a$") || strings.HasPrefix(encodedHash, "$2b$") {
		return verifyBcrypt(plaintext, encodedHash)
	}
	return VerifyAPIKey(plaintext, encodedHash)
}
