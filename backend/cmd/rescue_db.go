package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/term"
)

const dateInputLayout = "02-01-2006" // strict DD-MM-YYYY

func truncateHwidUserDevices(resources *rescueResources, reader *bufio.Reader) error {
	printStatus("◐", "🔄 Cleaning up HWID Devices...")

	answer, err := promptConfirm(reader, "Are you sure you want to clean up HWID Devices?")
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	ctx := context.Background()
	if _, err := resources.db.Exec(ctx, `TRUNCATE hwid_user_devices`); err != nil {
		return fmt.Errorf("clean up HWID Devices: %w", err)
	}

	printStatus("✔", "✅ HWID Devices cleaned up successfully.")

	return nil
}

func truncateSRHTable(resources *rescueResources, reader *bufio.Reader) error {
	printStatus("◐", "🔄 Cleaning up SRH Table...")

	answer, err := promptConfirm(reader, "Are you sure you want to clean up SRH Table?")
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	ctx := context.Background()
	if _, err := resources.db.Exec(ctx, `TRUNCATE user_subscription_request_history RESTART IDENTITY`); err != nil {
		return fmt.Errorf("clean up SRH Table: %w", err)
	}

	printStatus("✔", "✅ SRH Table cleaned up successfully.")

	return nil
}

func truncateUsersUsageTable(resources *rescueResources, reader *bufio.Reader) error {
	printStatus("◐", "🔄 Cleaning up Users Usage Table...")

	answer, err := promptConfirm(reader, "Are you sure you want to clean up Users Usage Table?")
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	ctx := context.Background()
	if _, err := resources.db.Exec(ctx, `TRUNCATE nodes_user_usage_history RESTART IDENTITY`); err != nil {
		return fmt.Errorf("clean up Users Usage Table: %w", err)
	}
	if _, err := resources.db.Exec(ctx, `VACUUM nodes_user_usage_history`); err != nil {
		return fmt.Errorf("vacuum Users Usage Table: %w", err)
	}
	if _, err := resources.db.Exec(ctx, `REINDEX TABLE nodes_user_usage_history`); err != nil {
		return fmt.Errorf("reindex Users Usage Table: %w", err)
	}

	printStatus("✔", "✅ Users Usage Table cleaned up successfully.")

	return nil
}

func promptStrictDate(reader *bufio.Reader, label string, example string) (time.Time, error) {
	fmt.Printf(
		"Enter the %s date in strict format day-month-year (DD-MM-YYYY), e.g. %s: ",
		label, example,
	)

	text, err := reader.ReadString('\n')
	if err != nil {
		return time.Time{}, fmt.Errorf("read %s date: %w", label, err)
	}

	parsed, err := time.Parse(dateInputLayout, strings.TrimSpace(text))
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"invalid %s date: expected strict format DD-MM-YYYY, e.g. %s", label, example,
		)
	}

	return parsed, nil
}

func promptBatchSize(reader *bufio.Reader) (int, error) {
	fmt.Print("Batch size (rows per DELETE) [50000]: ")

	text, err := reader.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("read batch size: %w", err)
	}

	text = strings.TrimSpace(text)
	if text == "" {
		text = "50000"
	}

	batchSize, err := strconv.Atoi(text)
	if err != nil || batchSize <= 0 {
		return 0, fmt.Errorf("invalid batch size: expected a positive integer")
	}

	return batchSize, nil
}

func formatThousands(n int64) string {
	s := strconv.FormatInt(n, 10)

	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}

	var out []byte
	for i, c := range []byte(s) {
		if i != 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}

	if neg {
		return "-" + string(out)
	}

	return string(out)
}

func renderDeleteProgress(current, total int64, startedAt time.Time, lastBatchMs int64) {
	ratio := 1.0
	if total > 0 {
		ratio = float64(current) / float64(total)
		if ratio > 1 {
			ratio = 1
		}
	}

	const width = 28
	filled := int(ratio*width + 0.5)
	bar := strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
	pct := fmt.Sprintf("%5.1f%%", ratio*100)

	elapsedSec := time.Since(startedAt).Seconds()
	etaStr := "—"
	if current > 0 && total > current {
		etaStr = fmt.Sprintf("%.1f", (elapsedSec/float64(current))*float64(total-current))
	}

	line := fmt.Sprintf(
		"  [%s] %s  %s/%s  | %.1fs elapsed | ETA %ss | last %dms | do NOT close",
		bar, pct, formatThousands(current), formatThousands(total), elapsedSec, etaStr, lastBatchMs,
	)

	if term.IsTerminal(int(os.Stdout.Fd())) {
		fmt.Print("\r\x1b[K")
		fmt.Print(line)
	} else {
		fmt.Println(strings.TrimSpace(line))
	}
}

func runSingleUsageDelete(resources *rescueResources, startStr, endStr string) (int64, error) {
	printStatus("◐", "🔄 Deleting records... (do NOT close this window)")

	ctx := context.Background()
	result, err := resources.db.Exec(ctx, `
		DELETE FROM nodes_user_usage_history
		WHERE created_at >= $1::date
		  AND created_at <= $2::date
	`, startStr, endStr)
	if err != nil {
		return 0, fmt.Errorf("delete records: %w", err)
	}

	deleted := result.RowsAffected()

	printStatus("✔", fmt.Sprintf("✅ Deleted %s record(s).", formatThousands(deleted)))

	return deleted, nil
}

func runBatchedUsageDelete(
	resources *rescueResources,
	startStr, endStr string,
	batchSize int,
	totalToDelete int64,
	startedAt time.Time,
) (int64, error) {
	var totalDeleted int64
	var timings []int64

	printStatus("◐", "🔄 Deleting records in batches... (do NOT close this window)")

	ctx := context.Background()
	for {
		batchStart := time.Now()

		result, err := resources.db.Exec(ctx, `
			DELETE FROM nodes_user_usage_history
			WHERE ctid IN (
				SELECT ctid
				FROM nodes_user_usage_history
				WHERE created_at >= $1::date
				  AND created_at <= $2::date
				LIMIT $3
			)
		`, startStr, endStr, batchSize)
		if err != nil {
			if term.IsTerminal(int(os.Stdout.Fd())) {
				fmt.Println()
			}
			return totalDeleted, fmt.Errorf("delete records: %w", err)
		}

		batchMs := time.Since(batchStart).Milliseconds()

		deleted := result.RowsAffected()

		if deleted == 0 {
			break
		}

		totalDeleted += deleted
		timings = append(timings, batchMs)

		renderDeleteProgress(totalDeleted, totalToDelete, startedAt, batchMs)
	}

	if term.IsTerminal(int(os.Stdout.Fd())) {
		fmt.Println()
	}

	if len(timings) > 0 {
		first := timings[0]
		last := timings[len(timings)-1]
		min, max, sum := timings[0], timings[0], int64(0)
		for _, t := range timings {
			if t < min {
				min = t
			}
			if t > max {
				max = t
			}
			sum += t
		}
		avg := sum / int64(len(timings))

		printInfoBox([]string{
			"Batched delete summary",
			fmt.Sprintf("batches:            %d", len(timings)),
			fmt.Sprintf("batch size:         %s", formatThousands(int64(batchSize))),
			fmt.Sprintf("deleted total:      %s", formatThousands(totalDeleted)),
			fmt.Sprintf("first / avg / last: %dms / %dms / %dms", first, avg, last),
			fmt.Sprintf("min / max:          %dms / %dms", min, max),
		})
	}

	return totalDeleted, nil
}

func deleteUsersUsageByDateRange(resources *rescueResources, reader *bufio.Reader) error {
	fmt.Println(
		"This will permanently delete users traffic statistics " +
			"(nodes_user_usage_history) for the selected date range.",
	)

	method, err := promptSelect([]cliAction{
		{
			Value: "single",
			Label: "Single query (fast)",
			Hint:  "One DELETE — fastest overall, but holds one longer lock",
		},
		{
			Value: "batched",
			Label: "Batched (low-lock + progress bar)",
			Hint:  "Many small DELETEs — shorter locks, live progress, slower overall",
		},
	}, 0)
	if err != nil {
		return err
	}

	startDate, err := promptStrictDate(reader, "START", "01-01-2024")
	if err != nil {
		return err
	}

	endDate, err := promptStrictDate(reader, "END", "31-12-2024")
	if err != nil {
		return err
	}

	if endDate.Before(startDate) {
		return fmt.Errorf("END date can not be earlier than START date")
	}

	batchSize := 0
	if method == "batched" {
		batchSize, err = promptBatchSize(reader)
		if err != nil {
			return err
		}
	}

	startStr := startDate.Format("2006-01-02")
	endStr := endDate.Format("2006-01-02")

	printStatus("◐", "🔍 Counting affected rows...")

	ctx := context.Background()
	var rowsToDelete int64
	if err := resources.db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM nodes_user_usage_history
		WHERE created_at >= $1::date
		  AND created_at <= $2::date
	`, startStr, endStr).Scan(&rowsToDelete); err != nil {
		return fmt.Errorf("count rows: %w", err)
	}

	if rowsToDelete == 0 {
		printStatus("ℹ", fmt.Sprintf(
			"ℹ️ No records found between %s and %s (inclusive). Nothing to delete.", startStr, endStr,
		))
		return nil
	}

	methodLine := "in a single query."
	if method == "batched" {
		methodLine = fmt.Sprintf("in batches of %s.", formatThousands(int64(batchSize)))
	}

	printInfoBox([]string{
		fmt.Sprintf("About to delete %s record(s)", formatThousands(rowsToDelete)),
		fmt.Sprintf("from %s to %s (inclusive)", startStr, endStr),
		"from table \"nodes_user_usage_history\" " + methodLine,
	})

	fmt.Println(
		"⚠ Do NOT close this window until the operation is finished.\n" +
			"⚠ A final VACUUM runs at the end to reclaim space.\n" +
			"⚠ Interrupting the operation may leave the table bloated.",
	)

	answer, err := promptConfirm(reader, fmt.Sprintf(
		"Are you sure you want to permanently delete these %s record(s)?", formatThousands(rowsToDelete),
	))
	if err != nil {
		return err
	}
	if !answer {
		return errors.New("aborted")
	}

	startedAt := time.Now()

	var totalDeleted int64
	if method == "batched" {
		totalDeleted, err = runBatchedUsageDelete(resources, startStr, endStr, batchSize, rowsToDelete, startedAt)
	} else {
		totalDeleted, err = runSingleUsageDelete(resources, startStr, endStr)
	}
	if err != nil {
		return err
	}

	printStatus("◐", "🧹 Reclaiming space (VACUUM)... (do NOT close this window)")
	if _, err := resources.db.Exec(ctx, `VACUUM nodes_user_usage_history`); err != nil {
		printStatus("⚠", fmt.Sprintf("⚠️ Final VACUUM failed (table left as-is): %v", err))
	}

	elapsedSec := time.Since(startedAt).Seconds()
	printStatus("✔", fmt.Sprintf(
		"✅ Done in %.1fs. Removed %s record(s) from %s to %s.",
		elapsedSec, formatThousands(totalDeleted), startStr, endStr,
	))

	return nil
}
