package worker

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type TickWorker interface {
	Start(stop chan bool, wg *sync.WaitGroup)
}

type tickWorker struct {
	*logrus.Entry

	name        string
	workers     int
	workersChan chan interface{}
	tickEvery   time.Duration
	// Returns count of processed records
	tick func(event func(interface{})) (int, error)
	// Processing data
	process func(interface{}, *logrus.Entry) error

	wg   *sync.WaitGroup
	stop chan bool
}

func NewWorker(
	name string,
	workers int,
	tickFunc func(event func(interface{})) (int, error),
	tickEvery time.Duration,
	processFunc func(interface{}, *logrus.Entry) error,
	logger *logrus.Entry,
) (*tickWorker, error) {
	return &tickWorker{
		Entry: logger,

		name:        name,
		workers:     workers,
		workersChan: make(chan interface{}, workers),
		tickEvery:   tickEvery,

		tick:    tickFunc,
		process: processFunc,

		wg:   &sync.WaitGroup{},
		stop: make(chan bool),
	}, nil
}

func (r *tickWorker) Start(stop chan bool, wg *sync.WaitGroup) {
	r.Debugf("%s worker starting", r.name)

	wg.Add(1)
	defer func() {
		r.Infof("%s worker stopped", r.name)
		wg.Done()
	}()

	// Building workers
	for i := 0; i < r.workers; i++ {
		go r.worker(i + 1)
	}

	// Stop signal listener
	go func(stop chan bool) {
		for {
			select {
			case <-stop:
				close(r.stop)
				return
			default:
				time.Sleep(time.Second)
			}
		}
	}(stop)

	r.Infof("%s worker started", r.name)

	// Tick loop
	timer := time.NewTimer(r.tickEvery)
	for {
		select {
		case <-stop:
			r.Infof("Page parser received stop signal. Waiting for inner goroutines")

			timer.Stop()
			r.wg.Wait()
			close(r.workersChan)
			return

		case <-timer.C:
			if count, err := r.tick(r.event); err != nil {
				r.Errorf("Tick failed - %s", err)
				timer = time.NewTimer(r.tickEvery * 2)
			} else if count > 0 {
				timer = time.NewTimer(time.Millisecond * 10)
			} else {
				r.Tracef("Sleep")
				timer = time.NewTimer(r.tickEvery)
			}

		default:
			time.Sleep(time.Second)
		}
	}
}

func (r *tickWorker) event(data interface{}) {
	select {
	case <-r.stop:
		return
	default:
		r.workersChan <- data
	}
}

func (r *tickWorker) worker(workerId int) {
	log := r.WithField("w", workerId)
	log.Debug("TickWorker started")

	r.wg.Add(1)
	defer func() {
		r.wg.Done()
		log.Infof("TickWorker %d stopped", workerId)
	}()

	for {
		select {
		case data := <-r.workersChan:
			if err := r.process(data, log); err != nil {
				log.Warnf("Processing failed - %s", err)
			} else {
				log.Debugf("Done")
			}

		case <-r.stop:
			// Clean chan
			select {
			case <-r.workersChan:
			default:
			}
			return

		default:
			time.Sleep(time.Second)
		}
	}
}
