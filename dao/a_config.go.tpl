package {{.DBName}}

import (
	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/redis"
	"github.com/ssgo/s"
	"time"
)


type Serve struct {
	conn *db.DB
	rd   *redis.Redis
}

func New(dbConn *db.DB, redisConn *redis.Redis) *Serve {
	serve := Serve{
		conn: dbConn,
		rd:   redisConn,
	}
	return &serve
}

func (serve *Serve) SetInject() {
	{{range .FixedTables}}
	s.SetInject(&{{.}}Dao{conn: serve.conn, rd: serve.rd}){{end}}
}

func (serve *Serve) NewTransaction(logger *log.Logger) *db.Tx {
	if logger != nil {
		return serve.conn.CopyByLogger(logger).Begin()
	}
	return serve.conn.Begin()
}

type Datetime string
type Time string
type Date string

func DatetimeByString(s string) Datetime {
	return Datetime(s)
}
func DatetimeByTime(t time.Time) Datetime {
	return Datetime(t.Format("2006-01-02 15:04:05"))
}
func (dt *Datetime) String() string {
	return string(*dt)
}
func (dt *Datetime) Time() time.Time {
	if t, err := time.Parse("2006-01-02 15:04:05", dt.String()); err == nil {
		return t
	} else {
		return time.Unix(0, 0)
	}
}

func TimeByString(s string) Time {
	return Time(s)
}
func TimeByTime(t time.Time) Time {
	return Time(t.Format("15:04:05"))
}
func (dt *Time) String() string {
	return string(*dt)
}
func (dt *Time) Time() time.Time {
	if t, err := time.Parse("15:04:05", dt.String()); err == nil {
		return t
	} else {
		return time.Unix(0, 0)
	}
}

func DateByString(s string) Date {
	return Date(s)
}
func DateByTime(t time.Time) Date {
	return Date(t.Format("2006-01-02"))
}
func (dt *Date) String() string {
	return string(*dt)
}
func (dt *Date) Time() time.Time {
	if t, err := time.Parse("2006-01-02", dt.String()); err == nil {
		return t
	} else {
		return time.Unix(0, 0)
	}
}
