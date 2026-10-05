package workerpool

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// collect читає всі результати з каналу до його закриття, але не
// довше за d — якщо канал так і не закрився, тест провалюється.
func collect(t *testing.T, results <-chan Result, d time.Duration) map[string]Result {
	t.Helper()
	got := make(map[string]Result)
	deadline := time.After(d)
	for {
		select {
		case r, ok := <-results:
			if !ok {
				return got
			}
			got[r.JobID] = r
		case <-deadline:
			t.Fatalf("канал результатів не закрився за %v", d)
			return nil
		}
	}
}

// TestRunPool_TimeoutReturnsDeadlineExceeded перевіряє, що повільне
// завдання, яке поважає ctx, завершується саме з
// context.DeadlineExceeded і нульовим Size.
func TestRunPool_TimeoutReturnsDeadlineExceeded(t *testing.T) {
	jobs := make(chan Job, 1)
	jobs <- Job{ID: "slow", Fetch: slowFetch(time.Second)}
	close(jobs)

	got := collect(t, RunPool(jobs, 1, 50*time.Millisecond), 2*time.Second)

	r, ok := got["slow"]
	if !ok {
		t.Fatal("немає результату для slow")
	}
	if !errors.Is(r.Err, context.DeadlineExceeded) {
		t.Errorf("Err = %v, want context.DeadlineExceeded", r.Err)
	}
	if r.Size != 0 {
		t.Errorf("Size = %d, want 0", r.Size)
	}
}

// TestRunPool_TimeoutWithNonCooperativeFetch перевіряє, що воркер не
// зависає, навіть якщо Fetch ігнорує ctx: результат з помилкою
// тайм-ауту має прийти приблизно за timeout, а не після завершення
// Fetch.
func TestRunPool_TimeoutWithNonCooperativeFetch(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	stubborn := func(ctx context.Context) (int, error) {
		<-release // ctx навмисно не перевіряється
		return 1, nil
	}

	jobs := make(chan Job, 1)
	jobs <- Job{ID: "stubborn", Fetch: stubborn}
	close(jobs)

	start := time.Now()
	got := collect(t, RunPool(jobs, 1, 100*time.Millisecond), 2*time.Second)
	elapsed := time.Since(start)

	if !errors.Is(got["stubborn"].Err, context.DeadlineExceeded) {
		t.Errorf("Err = %v, want context.DeadlineExceeded", got["stubborn"].Err)
	}
	if elapsed > time.Second {
		t.Errorf("пул тривав %v — мав завершитися близько тайм-ауту (100мс)", elapsed)
	}
}

// TestRunPool_TimeoutIsPerJob перевіряє, що тайм-аут рахується для
// кожного завдання окремо: повільні завдання падають з тайм-аутом,
// а швидкі в тому самому пулі завершуються успішно.
func TestRunPool_TimeoutIsPerJob(t *testing.T) {
	const timeout = 100 * time.Millisecond

	jobs := make(chan Job, 6)
	for i := 0; i < 3; i++ {
		jobs <- Job{ID: fmt.Sprintf("fast-%d", i), Fetch: slowFetch(10 * time.Millisecond)}
		jobs <- Job{ID: fmt.Sprintf("slow-%d", i), Fetch: slowFetch(time.Second)}
	}
	close(jobs)

	got := collect(t, RunPool(jobs, 3, timeout), 3*time.Second)

	if len(got) != 6 {
		t.Fatalf("отримано %d результатів, очікувалось 6", len(got))
	}
	for i := 0; i < 3; i++ {
		fast := got[fmt.Sprintf("fast-%d", i)]
		if fast.Err != nil || fast.Size != 1234 {
			t.Errorf("%s: Size = %d, Err = %v, want 1234, nil", fast.JobID, fast.Size, fast.Err)
		}
		slow := got[fmt.Sprintf("slow-%d", i)]
		if !errors.Is(slow.Err, context.DeadlineExceeded) {
			t.Errorf("%s: Err = %v, want context.DeadlineExceeded", slow.JobID, slow.Err)
		}
	}
}

// TestRunPool_FetchGetsDeadline перевіряє, що Fetch отримує контекст
// із дедлайном не пізніше за timeout від моменту старту завдання.
func TestRunPool_FetchGetsDeadline(t *testing.T) {
	const timeout = 200 * time.Millisecond

	deadlines := make(chan time.Time, 1)
	check := func(ctx context.Context) (int, error) {
		d, ok := ctx.Deadline()
		if !ok {
			return 0, errors.New("ctx без дедлайну")
		}
		deadlines <- d
		return 1, nil
	}

	jobs := make(chan Job, 1)
	jobs <- Job{ID: "check", Fetch: check}
	close(jobs)

	start := time.Now()
	got := collect(t, RunPool(jobs, 1, timeout), 2*time.Second)

	if err := got["check"].Err; err != nil {
		t.Fatalf("Err = %v", err)
	}
	if d := <-deadlines; d.Sub(start) > timeout+50*time.Millisecond {
		t.Errorf("дедлайн через %v після старту, want <= %v", d.Sub(start), timeout)
	}
}
