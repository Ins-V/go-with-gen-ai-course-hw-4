// Package workerpool реалізує Частину 3 домашньої роботи: пул
// воркерів з обмеженим часом на кожне завдання, що симулює
// конкурентне отримання розміру URL.
package workerpool

import (
	"context"
	"sync"
	"time"
)

// Job — одна одиниця роботи для пулу. Fetch отримує контекст із
// дедлайном і має сам його поважати (кооперативне скасування) —
// саме так уникають витоку горутин при тайм-ауті.
type Job struct {
	ID    string
	Fetch func(ctx context.Context) (int, error)
}

// Result — результат виконання одного Job.
type Result struct {
	JobID string
	Size  int
	Err   error
}

// RunPool запускає рівно numWorkers горутин-воркерів, які беруть
// завдання з jobs і надсилають Result у повернутий канал. Кожному
// Job надається не більше timeout часу — якщо job.Fetch не встигає,
// Result.Err міститиме помилку тайм-ауту (context.DeadlineExceeded).
//
// TODO (Завдання 3): реалізуйте цю функцію.
//   - запустіть рівно numWorkers горутин (використайте sync.WaitGroup,
//     щоб знати, коли всі вони завершили);
//   - кожен воркер у циклі `for job := range jobs` для кожного job:
//   - створює ctx, cancel := context.WithTimeout(context.Background(), timeout)
//     і викликає job.Fetch(ctx);
//   - обов'язково викликає cancel() (defer), щоб не тримати таймер;
//   - надсилає Result{JobID: job.ID, Size: size, Err: err} у results;
//   - у окремій горутині: після wg.Wait() закрийте results.
//
// Це і є той самий select + time.After / context.WithTimeout
// патерн проти витоку горутин, який ми проходили на занятті —
// різниця лише в тому, що тут скасування кооперативне: Fetch сам
// перевіряє ctx.Done() (дивіться приклад slowFetch у тестах).
func RunPool(jobs <-chan Job, numWorkers int, timeout time.Duration) <-chan Result {
	results := make(chan Result)
	var wg sync.WaitGroup

	wg.Add(numWorkers)
	for range numWorkers {
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- runJob(job, timeout)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

// fetchResult — те, що повертає job.Fetch, упаковане для передачі
// через канал.
type fetchResult struct {
	size int
	err  error
}

// runJob виконує один Job з обмеженням часу timeout. Воркер чекає на
// результат через select і повертається не пізніше дедлайну, навіть
// якщо job.Fetch ігнорує ctx. Канал done буферизований, тому горутина
// з Fetch не заблокується на надсиланні, коли воркер уже пішов далі.
func runJob(job Job, timeout time.Duration) Result {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan fetchResult, 1)
	go func() {
		size, err := job.Fetch(ctx)
		done <- fetchResult{size: size, err: err}
	}()

	select {
	case r := <-done:
		return Result{JobID: job.ID, Size: r.size, Err: r.err}
	case <-ctx.Done():
		return Result{JobID: job.ID, Err: ctx.Err()}
	}
}
