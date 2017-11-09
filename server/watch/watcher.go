package watch

// Watch implements water/v1/watch service
import (
	"github.com/Sirupsen/logrus"
	watcher "google.golang.org/genproto/googleapis/watcher/v1"
)

type WatchSrv struct {
	ActionChan chan watcher.Change
	CASChan    chan watcher.Change
}

func (s *WatchSrv) Watch(stream *watcher.Request, w watcher.Watcher_WatchServer) error {
	logrus.Infof("Starting watch for resource %s", stream.Target)
	for {
		select {
		case c := <-s.CASChan:
			if c.Element == stream.Target {
				if c.State == watcher.Change_EXISTS {
					w.Send(&watcher.ChangeBatch{
						Changes: []*watcher.Change{&c},
					})
					logrus.Infof("Send change update to watch: %s", c)
				}
			}
		case c := <-s.ActionChan:
			if c.Element == stream.Target {
				if c.State == watcher.Change_EXISTS {
					w.Send(&watcher.ChangeBatch{
						Changes: []*watcher.Change{&c},
					})
					logrus.Infof("Send change update to watch: %s", c)
				}
			}
		default:
		}
	}
	return nil
}
