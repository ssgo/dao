package {{.DBName}}

import (
	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/redis"
	"github.com/ssgo/s"
	"time"
)

var _conn *db.DB
var _rd *redis.Redis

func ConfigureDB(dsn string) {
	_conn = db.GetDB(dsn, nil)
}

func ConfigureRedis(dsn string) {
	_rd = redis.GetRedis(dsn, nil)
}

func ConfigureInjects() {
	{{range .FixedTables}}
	s.SetInject(&{{.}}Dao{conn: _conn, rd: _rd}){{end}}
}

func Configure(dbPool *db.DB, redisPool *redis.Redis, inject bool) {
	if dbPool != nil {
		_conn = dbPool
	}
	if redisPool != nil {
		_rd = redisPool
	}
	if inject {
		ConfigureInjects()
	}
}

func NewTransaction(logger *log.Logger) *db.Tx {
	if logger != nil {
		return _conn.CopyByLogger(logger).Begin()
	}
	return _conn.Begin()
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
