package vaultkeystore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/inklabs/rangedb/pkg/crypto"
)

type hashicorpVaultKeyStore struct {
	encryptor  crypto.Encryptor
	config     Config
	httpClient *http.Client
}

type Config struct {
	Address string
	Token   string
}

func New(config Config, encryptor crypto.Encryptor) (*hashicorpVaultKeyStore, error) {
	return &hashicorpVaultKeyStore{
		encryptor: encryptor,
		config:    config,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (h *hashicorpVaultKeyStore) Get(subjectID string) (string, error) {
	request, err := http.NewRequest(http.MethodGet, h.getPath(subjectID), nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("X-Vault-Token", h.config.Token)

	response, err := h.httpClient.Do(request)
	if err != nil {
		return "", err
	}

	var resp saveResponse
	err = json.NewDecoder(response.Body).Decode(&resp)
	if err != nil {
		return "", err
	}

	if response.StatusCode == http.StatusNotFound {
		if resp.Data.Metadata.Destroyed {
			return "", crypto.ErrKeyWasDeleted
		}
		return "", crypto.ErrKeyNotFound
	}

	if response.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(response.Body)
		log.Printf("%d: %s", response.StatusCode, body)

		return "", fmt.Errorf("unable to get encryption key")
	}

	return resp.Data.Data.Key, nil
}

func (h *hashicorpVaultKeyStore) Set(subjectID, key string) error {
	_, err := h.Get(subjectID)
	if err == nil {
		return crypto.ErrKeyExistsForSubjectID
	}

	payload := dataPayload{Data: map[string]string{
		"key": key,
	}}
	jsonString, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPost, h.getPath(subjectID), bytes.NewReader(jsonString))
	if err != nil {
		return err
	}
	request.Header.Set("X-Vault-Token", h.config.Token)

	response, err := h.httpClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(response.Body)
		log.Printf("%d: %s", response.StatusCode, body)

		return fmt.Errorf("unable to save")
	}

	return nil
}

func (h *hashicorpVaultKeyStore) Delete(subjectID string) error {
	jsonString := `{"versions":[1,2]}`
	request, err := http.NewRequest(http.MethodPost, h.getDestroyPath(subjectID), strings.NewReader(jsonString))
	if err != nil {
		return err
	}
	request.Header.Set("X-Vault-Token", h.config.Token)

	response, err := h.httpClient.Do(request)
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		body, _ := ioutil.ReadAll(response.Body)
		log.Printf("%d: %s", response.StatusCode, body)

		return fmt.Errorf("unable to delete encryption key")
	}

	return nil
}

type dataPayload struct {
	Data map[string]string `json:"data"`
}

type saveResponse struct {
	RequestID     string `json:"request_id"`
	LeaseID       string `json:"lease_id"`
	Renewable     bool   `json:"renewable"`
	LeaseDuration int    `json:"lease_duration"`
	Data          struct {
		Data struct {
			Key string `json:"key"`
		} `json:"data"`
		Metadata struct {
			CreatedTime  time.Time `json:"created_time"`
			DeletionTime string    `json:"deletion_time"`
			Destroyed    bool      `json:"destroyed"`
			Version      int       `json:"version"`
		} `json:"metadata"`
	} `json:"data"`
	WrapInfo interface{} `json:"wrap_info"`
	Warnings interface{} `json:"warnings"`
	Auth     interface{} `json:"auth"`
}

func (h *hashicorpVaultKeyStore) getPath(subjectID string) string {
	return fmt.Sprintf("%s/v1/secret/data/%s", h.config.Address, subjectID)
}

func (h *hashicorpVaultKeyStore) getDestroyPath(subjectID string) string {
	return fmt.Sprintf("%s/v1/secret/destroy/%s", h.config.Address, subjectID)
}

func (h *hashicorpVaultKeyStore) SetTimeout(duration time.Duration) {
	h.httpClient.Timeout = duration
}

func (h *hashicorpVaultKeyStore) SetTransport(transport http.RoundTripper) {
	h.httpClient.Transport = transport
}
