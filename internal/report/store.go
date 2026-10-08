package report

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const table = "sms_messages"

// Message یک ردیف گزارش پیامک برای همان tenant درخواست‌دهنده.
type Message struct {
	ID             string     `json:"id"`
	IdempotencyKey string     `json:"idempotency_key"`
	Kind           string     `json:"kind"`
	Recipient      string     `json:"recipient"`
	Sender         string     `json:"sender"`
	Body           string     `json:"body"`
	Cost           int64      `json:"cost"`
	Status         string     `json:"status"`
	DeadlineAt     *time.Time `json:"deadline_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// Page فهرست صفحه‌بندی‌شدهٔ پیام‌ها.
type Page struct {
	TenantID string    `json:"tenant_id"`
	From     time.Time `json:"from"`
	To       time.Time `json:"to"`
	Total    uint64    `json:"total"`
	Limit    int       `json:"limit"`
	Offset   int       `json:"offset"`
	Items    []Message `json:"items"`
}

// Summary جمع وضعیت و هزینه در همان بازه.
type Summary struct {
	TenantID     string    `json:"tenant_id"`
	From         time.Time `json:"from"`
	To           time.Time `json:"to"`
	Total        uint64    `json:"total"`
	Accepted     uint64    `json:"accepted"`
	Rejected     uint64    `json:"rejected"`
	Refunded     uint64    `json:"refunded"`
	AcceptedCost int64     `json:"accepted_cost"`
	RefundedCost int64     `json:"refunded_cost"`
	NetCost      int64     `json:"net_cost"`
}

// Store خواندن گزارش از کلیک‌هوس. نوشتن این‌جا نیست.
type Store struct {
	conn clickhouse.Conn
}

func NewStore(conn clickhouse.Conn) *Store {
	return &Store{conn: conn}
}

// Open با مقادیری که config.Load از env خوانده وصل می‌شود.
func Open(ctx context.Context, addr, database, user, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err = conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}

// List پیام‌های همان tenant را در بازه برمی‌گرداند. FINAL آخرین نسخهٔ هر کلید را می‌دهد.
func (s *Store) List(ctx context.Context, f Filter) (Page, error) {
	where, args := f.where()
	page := Page{
		TenantID: f.TenantID,
		From:     f.From,
		To:       f.To,
		Limit:    f.Limit,
		Offset:   f.Offset,
		Items:    []Message{},
	}

	countQuery := "SELECT count() FROM " + table + " FINAL WHERE " + where
	if err := s.conn.QueryRow(ctx, countQuery, args...).Scan(&page.Total); err != nil {
		return Page{}, fmt.Errorf("count messages: %w", err)
	}
	if page.Total == 0 {
		return page, nil
	}

	listQuery := `
		SELECT
			toString(id),
			idempotency_key,
			kind,
			recipient,
			sender,
			body,
			cost,
			event_type,
			deadline_at,
			created_at
		FROM ` + table + ` FINAL
		WHERE ` + where + `
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?`
	listArgs := append(append([]any{}, args...), f.Limit, f.Offset)
	rows, err := s.conn.Query(ctx, listQuery, listArgs...)
	if err != nil {
		return Page{}, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			item      Message
			eventType string
		)
		if err = rows.Scan(
			&item.ID,
			&item.IdempotencyKey,
			&item.Kind,
			&item.Recipient,
			&item.Sender,
			&item.Body,
			&item.Cost,
			&eventType,
			&item.DeadlineAt,
			&item.CreatedAt,
		); err != nil {
			return Page{}, fmt.Errorf("scan message: %w", err)
		}
		item.Status = strings.TrimPrefix(eventType, "message.")
		page.Items = append(page.Items, item)
	}
	if err = rows.Err(); err != nil {
		return Page{}, fmt.Errorf("read messages: %w", err)
	}
	return page, nil
}

// Summary تعداد هر وضعیت و هزینهٔ خالص accepted منهای refunded.
func (s *Store) Summary(ctx context.Context, f Filter) (Summary, error) {
	where, args := f.where()
	query := `
		SELECT
			count() AS total,
			countIf(event_type = 'message.accepted') AS accepted,
			countIf(event_type = 'message.rejected') AS rejected,
			countIf(event_type = 'message.refunded') AS refunded,
			sumIf(cost, event_type = 'message.accepted') AS accepted_cost,
			sumIf(cost, event_type = 'message.refunded') AS refunded_cost
		FROM ` + table + ` FINAL
		WHERE ` + where

	out := Summary{TenantID: f.TenantID, From: f.From, To: f.To}
	if err := s.conn.QueryRow(ctx, query, args...).Scan(
		&out.Total,
		&out.Accepted,
		&out.Rejected,
		&out.Refunded,
		&out.AcceptedCost,
		&out.RefundedCost,
	); err != nil {
		return Summary{}, fmt.Errorf("summary: %w", err)
	}
	out.NetCost = out.AcceptedCost - out.RefundedCost
	return out, nil
}

func (f Filter) where() (string, []any) {
	clauses := []string{
		"tenant_id = ?",
		"created_at >= ?",
		"created_at < ?",
	}
	args := []any{f.TenantID, f.From.UTC(), f.To.UTC()}
	if f.Kind != "" {
		clauses = append(clauses, "kind = ?")
		args = append(args, f.Kind)
	}
	if eventType := f.eventType(); eventType != "" {
		clauses = append(clauses, "event_type = ?")
		args = append(args, eventType)
	}
	return strings.Join(clauses, " AND "), args
}
