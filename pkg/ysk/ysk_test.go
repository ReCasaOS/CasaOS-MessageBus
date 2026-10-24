package ysk_test

import (
	"context"
	"testing"
	"time"

	"github.com/ReCasaOS/CasaOS-Common/utils/logger"
	"github.com/ReCasaOS/CasaOS-MessageBus/model"
	"github.com/ReCasaOS/CasaOS-MessageBus/pkg/ysk"
	"github.com/ReCasaOS/CasaOS-MessageBus/repository"
	"github.com/ReCasaOS/CasaOS-MessageBus/service"
	"github.com/ReCasaOS/CasaOS-MessageBus/utils"
	"gotest.tools/assert"
)

var ws *service.EventServiceWS

func setup(t *testing.T) (*service.EventServiceWS, *service.YSKService, func()) {
	repository, err := repository.NewDatabaseRepositoryInMemory()
	assert.NilError(t, err)
	s := service.NewServices(&repository)
	wsService := s.EventServiceWS
	yskService := s.YSKService

	ctx := context.Background()
	go s.Start(&ctx)
	<-wsService.Ready()

	return wsService, yskService, func() {
		repository.Close()
	}
}

// waitForCardCount polls the card list until it holds want cards. Cards are
// stored by a background goroutine, so the delay depends on machine load: a
// fixed sleep fails on a busy CI runner.
func waitForCardCount(t *testing.T, yskService *service.YSKService, want int) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		cards, err := yskService.YskCardList(context.Background())
		assert.NilError(t, err)
		if len(cards) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d cards within 10s, got %d", want, len(cards))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mockPublish(ctx context.Context, sourceID string, eventName string, body map[string]string) {
	if ws != nil {
		ws.Publish(model.Event{
			SourceID:   sourceID,
			Name:       eventName,
			Properties: body,
		})
	}
}

func TestUpdateProgress(t *testing.T) {
	logger.LogInitConsoleOnly()

	wsService, yskService, cleanup := setup(t)
	defer cleanup()
	ws = wsService

	yskService.Start(false)

	err := ysk.NewYSKCard(context.Background(), utils.ApplicationInstallProgress.WithTaskContent(
		"jellyfin logo",
		"Installing LinuxServer/Jellyfin",
	).WithProgress(
		"Installing LinuxServer/Jellyfin", 25,
	), mockPublish)
	assert.NilError(t, err)

	err = ysk.NewYSKCard(context.Background(), utils.ApplicationInstallProgress.WithProgress(
		"Installing LinuxServer/Jellyfin", 50,
	), mockPublish)
	assert.NilError(t, err)

	waitForCardCount(t, yskService, 1)

	err = ysk.DeleteCard(context.Background(), utils.ApplicationInstallProgress.Id, mockPublish)
	assert.NilError(t, err)

	waitForCardCount(t, yskService, 0)
}

func TestLongAndShortNoticeInsert(t *testing.T) {
	logger.LogInitConsoleOnly()

	wsService, yskService, cleanup := setup(t)
	defer cleanup()
	ws = wsService

	yskService.Start(false)

	err := ysk.NewYSKCard(context.Background(), utils.ZimaOSDataStationNotice, mockPublish)
	assert.NilError(t, err)
	err = ysk.NewYSKCard(context.Background(), utils.ApplicationUpdateNotice, mockPublish)
	assert.NilError(t, err)

	// only the long notice is stored; the short notice must not add a card
	waitForCardCount(t, yskService, 1)
}
