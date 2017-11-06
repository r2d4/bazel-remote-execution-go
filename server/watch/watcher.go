package watch

// Watch implements water/v1/watch service
import watcher "google.golang.org/genproto/googleapis/watcher/v1"

type WatchSrv struct {
	ActionChan chan watcher.Change
	CASChan    chan watcher.Change
}

func (s *WatchSrv) Watch(stream *watcher.Request, w watcher.Watcher_WatchServer) error {
	for {
		// Send finish message!
	}
	return nil
}
