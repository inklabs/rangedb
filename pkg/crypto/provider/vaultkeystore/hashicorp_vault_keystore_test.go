package vaultkeystore_test

import (
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/vaultkeystore"
)

func TestHashicorpVault_VerifyEngineInterface(t *testing.T) {
	config := getConfigFromEnvironment(t)
	const iv = "1234567890123456"
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))

	cryptotest.VerifyKeyStore(t, func(t *testing.T) crypto.KeyStore {
		vaultKeyStore, err := vaultkeystore.New(config, aesEncryptor)
		require.NoError(t, err)
		return vaultKeyStore
	})
}

func TestFailures(t *testing.T) {
	config := getConfigFromEnvironment(t)
	const (
		iv            = "1234567890123456"
		subjectID     = "eb7ea7b5ec984f4893b9a0ae29efb99b"
		encryptionKey = "d67d3e84841c4ab58db32ae694ea7cad"
	)
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))

	t.Run("get", func(t *testing.T) {
		t.Run("errors from invalid subjectID", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			const invalidSubjectID = ":/#?&@%+~"

			// When
			key, err := vaultCrypto.Get(invalidSubjectID)

			// Then
			require.EqualError(t, err, `parse "http://127.0.0.1:8200/v1/secret/data/:/#?&@%+~": invalid URL escape "%+~"`)
			assert.Equal(t, "", key)
		})

		t.Run("errors from http timeout", func(t *testing.T) {
			// Given
			config.Address = "http://192.0.2.1:8200"
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTimeout(time.Nanosecond)

			// When
			key, err := vaultCrypto.Get(subjectID)

			// Then
			require.EqualError(t, err, `Get "http://192.0.2.1:8200/v1/secret/data/eb7ea7b5ec984f4893b9a0ae29efb99b": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`)
			assert.Equal(t, "", key)
		})

		t.Run("errors from invalid json response", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTransport(NewStubTransport(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(strings.NewReader("{invalid-json")),
				}, nil
			}))

			// When
			key, err := vaultCrypto.Get(subjectID)

			// Then
			require.EqualError(t, err, "invalid character 'i' looking for beginning of object key string")
			assert.Equal(t, "", key)
		})

		t.Run("errors when getting encryption key", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTransport(NewStubTransport(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusRequestTimeout,
					Body:       ioutil.NopCloser(strings.NewReader("{}")),
				}, nil
			}))

			// When
			key, err := vaultCrypto.Get(subjectID)

			// Then
			require.EqualError(t, err, "unable to get encryption key")
			assert.Equal(t, "", key)
		})
	})

	t.Run("set", func(t *testing.T) {
		t.Run("errors from invalid subjectID", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			const invalidSubjectID = ":/#?&@%+~"

			// When
			err = vaultCrypto.Set(invalidSubjectID, encryptionKey)

			// Then
			require.EqualError(t, err, `parse "http://192.0.2.1:8200/v1/secret/data/:/#?&@%+~": invalid URL escape "%+~"`)
		})

		t.Run("errors from http timeout", func(t *testing.T) {
			// Given
			config.Address = "http://192.0.2.1:8200"
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTimeout(time.Nanosecond)

			// When
			err = vaultCrypto.Set(subjectID, encryptionKey)

			// Then
			require.EqualError(t, err, `Post "http://192.0.2.1:8200/v1/secret/data/eb7ea7b5ec984f4893b9a0ae29efb99b": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`)
		})

		t.Run("errors when saving encryption key", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTransport(NewStubTransport(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       ioutil.NopCloser(strings.NewReader("{}")),
				}, nil
			}))

			// When
			err = vaultCrypto.Set(subjectID, encryptionKey)

			// Then
			require.EqualError(t, err, "unable to save")
		})
	})

	t.Run("delete", func(t *testing.T) {
		t.Run("delete errors from invalid subjectID", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			const invalidSubjectID = ":/#?&@%+~"

			// When
			err = vaultCrypto.Delete(invalidSubjectID)

			// Then
			require.EqualError(t, err, `parse "http://192.0.2.1:8200/v1/secret/destroy/:/#?&@%+~": invalid URL escape "%+~"`)
		})

		t.Run("errors from http timeout", func(t *testing.T) {
			// Given
			config.Address = "http://192.0.2.1:8200"
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTimeout(time.Nanosecond)

			// When
			err = vaultCrypto.Delete(subjectID)

			// Then
			require.EqualError(t, err, `Post "http://192.0.2.1:8200/v1/secret/destroy/eb7ea7b5ec984f4893b9a0ae29efb99b": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`)
		})

		t.Run("errors when deleting encryption key", func(t *testing.T) {
			// Given
			vaultCrypto, err := vaultkeystore.New(config, aesEncryptor)
			require.NoError(t, err)
			vaultCrypto.SetTransport(NewStubTransport(func(request *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusRequestTimeout,
					Body:       ioutil.NopCloser(strings.NewReader("{}")),
				}, nil
			}))

			// When
			err = vaultCrypto.Delete(subjectID)

			// Then
			require.EqualError(t, err, "unable to delete encryption key")
		})
	})
}

func getConfigFromEnvironment(t *testing.T) vaultkeystore.Config {
	address := os.Getenv("VAULT_ADDRESS")
	token := os.Getenv("VAULT_TOKEN")

	if address == "" || token == "" {
		// docker run -p 8200:8200 -e 'VAULT_DEV_ROOT_TOKEN_ID=testroot' vault
		t.Skip("VAULT_ADDRESS and VAULT_TOKEN are required")
	}

	return vaultkeystore.Config{
		Address: address,
		Token:   token,
	}
}

type stubTransport struct {
	roundTrip func(request *http.Request) (*http.Response, error)
}

func NewStubTransport(roundTrip func(request *http.Request) (*http.Response, error)) *stubTransport {
	return &stubTransport{
		roundTrip: roundTrip,
	}
}

func (s *stubTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return s.roundTrip(request)
}
