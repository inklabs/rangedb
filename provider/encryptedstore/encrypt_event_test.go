package encryptedstore_test

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/eventencryptor"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/provider/encryptedstore"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func ExampleNew_automatically_encrypt_decrypt() {
	// Given
	seededRandReader := rand.New(rand.NewSource(100))
	aesEncryptor := aes.NewGCM()
	aesEncryptor.SetRandReader(seededRandReader)
	keyStore := inmemorykeystore.New()
	eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
	eventEncryptor.SetRandReader(seededRandReader)
	event := &cryptotest.CustomerSignedUp{
		ID:     "fe65ac8d8c3a45e9b3cee407f10ee518",
		Name:   "John Doe",
		Email:  "john@example.com",
		Status: "active",
	}
	inMemoryStore := inmemorystore.New()
	encryptedStore := encryptedstore.New(inMemoryStore, eventEncryptor)
	encryptedStore.Bind(&cryptotest.CustomerSignedUp{})

	ctx := context.Background()

	// When
	_, err := encryptedStore.Save(ctx, &rangedb.EventRecord{Event: event})
	PrintError(err)

	fmt.Println("Auto Decrypted Event:")
	recordIterator := encryptedStore.Events(ctx, 0)
	for recordIterator.Next() {
		PrintEvent(recordIterator.Record().Data.(rangedb.Event))
	}

	fmt.Println("Raw Encrypted Event:")
	rawRecordIterator := inMemoryStore.Events(ctx, 0)
	for rawRecordIterator.Next() {
		PrintEvent(rawRecordIterator.Record().Data.(rangedb.Event))
	}

	// Output:
	// Auto Decrypted Event:
	// {
	//   "ID": "fe65ac8d8c3a45e9b3cee407f10ee518",
	//   "Name": "John Doe",
	//   "Email": "john@example.com",
	//   "Status": "active"
	// }
	// Raw Encrypted Event:
	// {
	//   "ID": "fe65ac8d8c3a45e9b3cee407f10ee518",
	//   "Name": "Lp5pGK8QGYw3NJyJVBsW49HESSf+NEraAQoBmpLXboZvsN/L",
	//   "Email": "o1H9t1BClYc5UcyUV+Roe3wz5gwRZRjgBI/xzwZs8ueQGQ5L8uGnbrTGrh8=",
	//   "Status": "active"
	// }
}
