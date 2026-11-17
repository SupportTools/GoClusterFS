package api

import (
	"os"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/testnet"
)

func TestMain(m *testing.M) {
	listen = testnet.Listen
	os.Exit(m.Run())
}
