package db

import (
	"testing"

	"github.com/card-engine/common/utils"
)

func TestRedis(t *testing.T) {
	dsn := "redis://localhost:6379/0?ssl=true&skip_verify=true"
	client, err := utils.InitRedisByDNS(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
}
