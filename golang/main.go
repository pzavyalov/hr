package main

import (
	"fmt"
	"sync"
	"time"
)

// ЗАДАНИЕ:
// * сделать из плохого кода хороший;
// * важно сохранить логику появления ошибочных тасков;
// * сделать правильную мультипоточность обработки заданий.
// Обновленный код отправить через merge-request.

// приложение эмулирует получение и обработку тасков, пытается и получать и обрабатывать в многопоточном режиме
// В конце должно выводить успешные таски и ошибки выполнены остальных тасков


// Комментарии:
// количество воркеров можно настраивать
// не стал добавлять таймер на предельное время работы воркера, так как он гарантированно завершиться при такой реализации
// не понятно как собирать результаты, но учитывая что они выводятся после того как все отработает, оставил как есть
// не понятно что должно быть в taskResult, оставил тоже то что и писалось, добавил признак успешности выполнения таска.
// поменял генерацию id таска, так как при старой реализации они дублировались
// если оставлять текущую логику, ошибочных тасков не генерируется, только если поменять условие

var workersCtn = 10

type Task struct {
	ID        int64
	CreatedAt time.Time     // время создания
	Duration  time.Duration // время выполнения
	Result    []byte
	IsSuccess bool
}

func main() {
	var errs []error
	result := map[int64]Task{}
	mu := sync.Mutex{}
	collectorCh := make(chan Task)
	doneCh := make(chan struct{})

	go taskCreator(collectorCh, doneCh)

	wg := sync.WaitGroup{}
	for i := 0; i < workersCtn; i++ { // запускаем пулл горутин на обработку тасков
		wg.Add(1)
		go func() {
			// получение тасков
			for t := range collectorCh {
				t := taskWorker(t)
				if t.IsSuccess {
					mu.Lock()
					result[t.ID] = t
					mu.Unlock()
				} else {
					errs = append(errs, fmt.Errorf("Task ID %d time %s, error %s", t.ID, t.CreatedAt, t.Result))
				}
			}
			wg.Done()
		}()
	}

	time.Sleep(time.Second * 3)
	doneCh <- struct{}{} // закроет канал коллектор, го-рутины начнут выходить из цикла
	wg.Wait() // дожидаемся когда все горутины доработают

	println("Errors:")
	for _, r := range errs {
		println(r.Error())
	}

	println("Done tasks:")
	for r := range result {
		println(r)
	}
}

func taskCreator(collectorCh chan Task, doneCh chan struct{}) {
	for {
		select {
		case <- doneCh:
			close(collectorCh)
			return
		default:
			createdAt := time.Now()
			if createdAt.Nanosecond()%2 > 0 { // вот такое условие появления ошибочных тасков
				createdAt = time.Now().Add(-30 * time.Second)
			}
			collectorCh <- Task{CreatedAt: createdAt, ID: time.Now().UnixMicro()} // передаем таск на выполнение
		}
	}
}

func taskWorker(task Task) Task {
	startedAt := time.Now()

	if task.CreatedAt.After(time.Now().Add(-20 * time.Second)) {
		task.Result = []byte("task has been successed")
		task.IsSuccess = true
	} else {
		task.Result = []byte("something went wrong")
	}

	task.Duration = time.Now().Sub(startedAt)

	time.Sleep(time.Millisecond * 150)

	return task
}
