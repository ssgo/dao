package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ssgo/db"
	"github.com/ssgo/u"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strings"
	"text/template"
)

type ValidFieldConfig struct {
	Field           string
	Type            string
	ValidOperator   string
	ValidValue      string
	InvalidOperator string
	InvalidValue    string
}

type DaoConfig struct {
	VersionField string
	ValidFields  []ValidFieldConfig
	Db           []string
}

type TableDesc struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
}

type TableIndex struct {
	Non_unique   int
	Key_name     string
	Seq_in_index int
	Column_name  string
}

type DaoData struct {
	DBName       string
	VersionField string
	Tables       []string
	FixedTables  []string
}

var configTpl = `package {{.DBName}}

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

`

//type AA string
//
//const (
//	A1 AA = "111"
//	A2 AA = "222"
//)


type FieldData struct {
	Name    string
	Type    string
	Default string
	Options map[string]string
}

type IndexField struct {
	Name       string
	Where      string
	Args       string
	Params     string
	ItemArgs   string
	StringArgs string
}

type TableData struct {
	DBName         string
	TableName      string
	FixedTableName string
	IsAutoId       bool
	PrimaryKey     *IndexField
	UniqueKeys     map[string]*IndexField
	IndexKeys      map[string]*IndexField
	Fields         []FieldData
	PointFields    []FieldData
	//FieldsWithoutAutoId []FieldData
	SelectFields string
	ValidField   string
	ValidWhere   string
	ValidSet     string
	InvalidSet   string
	VersionField string
	HasVersion   bool
}

var tableTpl = `package {{.DBName}}

import (
	"fmt"
	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/redis"
	"github.com/ssgo/u"
	"reflect"
	"strings"
)

type {{.FixedTableName}}Dao struct {
	conn *db.DB
	tx *db.Tx
	rd *redis.Redis
	logger *log.Logger
	lastError error
}

func Get{{.FixedTableName}}Dao(logger *log.Logger) *{{.FixedTableName}}Dao {
	if _conn == nil {
		log.DefaultLogger.Error("no db configured", "dao", "{{.DBName}}", "table", "{{.TableName}}")
		return nil
	}

	conn := _conn
	rd := _rd
	if logger != nil {
		conn = _conn.CopyByLogger(logger)
		if rd != nil {
			rd = _rd.CopyByLogger(logger)
		}
	}

	return &{{.FixedTableName}}Dao{
		conn: conn,
		tx: nil,
		rd: rd,
	}
}

func Get{{.FixedTableName}}DaoByTransaction(tx *db.Tx, logger *log.Logger) *{{.FixedTableName}}Dao {
	dao := Get{{.FixedTableName}}Dao(logger)
	dao.tx = tx
	return dao
}

func (dao *{{.FixedTableName}}Dao) CopyByLogger(logger *log.Logger) *{{.FixedTableName}}Dao {
	newDao := new({{.FixedTableName}}Dao)
	if logger == nil {
		logger = log.DefaultLogger
	}
	newDao.logger = logger
	if dao.conn != nil {
		newDao.conn = dao.conn.CopyByLogger(logger)
	}
	if dao.tx != nil {
		newDao.tx = dao.tx
	}
	if dao.rd != nil {
		newDao.rd = dao.rd.CopyByLogger(logger)
	}
	return newDao
}

func (dao *{{.FixedTableName}}Dao) NewTransaction() (*{{.FixedTableName}}Dao, *db.Tx) {
	newDao := dao.CopyByLogger(dao.logger)
	newDao.tx = newDao.conn.Begin()
	return newDao, newDao.tx
}

func (dao *{{.FixedTableName}}Dao) LastError() error {
	return dao.lastError
}

func (dao *{{.FixedTableName}}Dao) GetConnection() *db.DB {
	return dao.conn
}

func (dao *{{.FixedTableName}}Dao) New() *{{.FixedTableName}}Item {
	return &{{.FixedTableName}}Item{dao: dao}
}

func (dao *{{.FixedTableName}}Dao) Attach(item *{{.FixedTableName}}Item) {
	item.dao = dao
}

{{range .UniqueKeys}}
func (dao *{{$.FixedTableName}}Dao) GetBy{{.Name}}({{.Params}}) *{{$.FixedTableName}}Item {
	result := make([]{{$.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT {{$.SelectFields}} FROM ` + "`" + `{{$.TableName}}` + "`" + ` WHERE {{.Where}}{{$.ValidWhere}}", {{.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT {{$.SelectFields}} FROM ` + "`" + `{{$.TableName}}` + "`" + ` WHERE {{.Where}}{{$.ValidWhere}}", {{.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		return &result[0]
	}
	return nil
}
{{ end }}

{{ if .PrimaryKey }}
func (dao *{{.FixedTableName}}Dao) Get({{.PrimaryKey.Params}}) *{{.FixedTableName}}Item {
	result := make([]{{.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT {{.SelectFields}} FROM ` + "`" + `{{.TableName}}` + "`" + ` WHERE {{.PrimaryKey.Where}}{{.ValidWhere}}", {{.PrimaryKey.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT {{.SelectFields}} FROM ` + "`" + `{{.TableName}}` + "`" + ` WHERE {{.PrimaryKey.Where}}{{.ValidWhere}}", {{.PrimaryKey.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		return &result[0]
	}
	return nil
}

{{ if .ValidSet }}
func (dao *{{.FixedTableName}}Dao) GetWithInvalid({{.PrimaryKey.Params}}) *{{.FixedTableName}}Item {
	result := make([]{{.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT {{.SelectFields}} FROM ` + "`" + `{{.TableName}}` + "`" + ` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT {{.SelectFields}} FROM ` + "`" + `{{.TableName}}` + "`" + ` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		return &result[0]
	}
	return nil
}
{{ end }}

{{ end }}

{{ if .IsAutoId }}
func (dao *{{.FixedTableName}}Dao) Insert(item *{{.FixedTableName}}Item) (int64, bool) {
{{ else }}
func (dao *{{.FixedTableName}}Dao) Insert(item *{{.FixedTableName}}Item) bool {
{{ end }}
{{ if .HasVersion }}
	item.Version = dao.getVersion()
{{ end }}
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Insert("{{.TableName}}", item)
	} else {
		r = dao.conn.Insert("{{.TableName}}", item)
	}
{{ if .HasVersion }}
	dao.commitVersion(item.Version)
{{ end }}
	dao.lastError = r.Error
{{ if .IsAutoId }}
	return r.Id(), r.Error == nil && r.Changes() > 0
{{ else }}
	return r.Error == nil && r.Changes() > 0
{{ end }}
}

func (dao *{{.FixedTableName}}Dao) Replace(item *{{.FixedTableName}}Item) bool {
{{ if .HasVersion }}
	item.Version = dao.getVersion()
{{ end }}
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Replace("{{.TableName}}", item)
	} else {
		r = dao.conn.Replace("{{.TableName}}", item)
	}
{{ if .HasVersion }}
	dao.commitVersion(item.Version)
{{ end }}
	return r.Error == nil && r.Changes() > 0
}

{{ if .PrimaryKey }}

func (dao *{{.FixedTableName}}Dao) Update(data interface{}, {{.PrimaryKey.Params}}) bool {
{{ if .HasVersion }}
	updateData, ok := data.(map[string]interface{})
	if !ok {
		updateData = make(map[string]interface{})
		u.Convert(data, updateData)
	}
	version := dao.getVersion()
	updateData["{{.VersionField}}"] = version
	data = updateData
{{ end }}
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Update("{{.TableName}}", data, "{{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Update("{{.TableName}}", data, "{{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
{{ if .HasVersion }}
	dao.commitVersion(version)
{{ end }}
	dao.lastError = r.Error
	return r.Error == nil && r.Changes() > 0
}

{{ if .InvalidSet }}
func (dao *{{.FixedTableName}}Dao) Enable({{.PrimaryKey.Params}}) bool {
	var r *db.ExecResult
{{ if .HasVersion }}
	version := dao.getVersion()
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.ValidSet}}, ` + "`" + `{{.VersionField}}` + "`" + `=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.ValidSet}}, ` + "`" + `{{.VersionField}}` + "`" + `=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	}
	dao.commitVersion(version)
{{ else }}
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.ValidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.ValidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
{{ end }}
	dao.lastError = r.Error
	return r.Error == nil && r.Changes() > 0
}

func (dao *{{.FixedTableName}}Dao) Disable({{.PrimaryKey.Params}}) bool {
	var r *db.ExecResult
{{ if .HasVersion }}
	version := dao.getVersion()
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.InvalidSet}}, ` + "`" + `{{.VersionField}}` + "`" + `=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.InvalidSet}}, ` + "`" + `{{.VersionField}}` + "`" + `=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	}
	dao.commitVersion(version)
{{ else }}
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.InvalidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE ` + "`" + `{{.TableName}}` + "`" + ` SET {{.InvalidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
{{ end }}
	dao.lastError = r.Error
	return r.Error == nil && r.Changes() > 0
}
{{ end }}

func (dao *{{.FixedTableName}}Dao) Delete({{.PrimaryKey.Params}}) bool {
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Exec("DELETE FROM ` + "`" + `{{.TableName}}` + "`" + ` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("DELETE FROM ` + "`" + `{{.TableName}}` + "`" + ` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
	dao.lastError = r.Error
	return r.Error == nil && r.Changes() > 0
}

{{ end }}

func (dao *{{.FixedTableName}}Dao) UpdateBy(data interface{}, where string, args ...interface{}) bool {
{{ if .HasVersion }}
	updateData, ok := data.(map[string]interface{})
	if !ok {
		updateData = make(map[string]interface{})
		u.Convert(data, updateData)
	}
	version := dao.getVersion()
	updateData["{{.VersionField}}"] = version
	data = updateData
{{ end }}
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Update("{{.TableName}}", data, where, args...)
	} else {
		r = dao.conn.Update("{{.TableName}}", data, where, args...)
	}
{{ if .HasVersion }}
	dao.commitVersion(version)
{{ end }}
	dao.lastError = r.Error
	return r.Error == nil && r.Changes() > 0
}

{{ if .HasVersion }}
func (dao *{{.FixedTableName}}Dao) getVersion() uint64 {
	var version uint64 = 0
	if dao.rd != nil {
		version = uint64(dao.rd.INCR("_DATA_VERSION_{{.TableName}}"))
		if version > 1 {
			// 设置使用中的标记
			dao.rd.SETEX("_DATA_VERSION_DOING_{{.TableName}}_"+u.String(version), 10, true)
			return version
		}

		// 不存在redis数据时，使用数据库中的版本重建
		dao.rd.DEL("_DATA_VERSION_{{.TableName}}")
		version = 0
	} else {
		dao.logger.Warning("use version but not configured redis", "dao", "{{.DBName}}", "table", "{{.TableName}}")
	}

	var r *db.Result
	if dao.tx != nil {
		r = dao.conn.Query("SELECT MAX(` + "`" + `{{.VersionField}}` + "`" + `) FROM ` + "`" + `{{.TableName}}` + "`" + `")
	} else {
		r = dao.conn.Query("SELECT MAX(` + "`" + `{{.VersionField}}` + "`" + `) FROM ` + "`" + `{{.TableName}}` + "`" + `")
	}
	maxVersion := uint64(r.IntOnR1C1())
	version = maxVersion+1
	if dao.rd != nil {
		// redis中信息丢失时从数据库中更新
		dao.rd.MSET("_DATA_VERSION_{{.TableName}}", version, "_DATA_MAX_VERSION_{{.TableName}}", version)
		// 设置使用中的标记
		dao.rd.SETEX("_DATA_VERSION_DOING_{{.TableName}}_"+u.String(version), 10, true)
	}
	return version
}

func (dao *{{.FixedTableName}}Dao) commitVersion(version uint64) {
	if dao.rd != nil {
		// 先存储当前版本完成标记，然后检查所有新版本是否完成以设置MAX_VERSION
		dao.rd.DEL("_DATA_VERSION_DOING_{{.TableName}}_"+u.String(version))
		seqVersion := dao.rd.GET("_DATA_VERSION_{{.TableName}}").Uint64()
		currentMaxVersion := dao.rd.GET("_DATA_MAX_VERSION_{{.TableName}}").Uint64()
		for i := currentMaxVersion; i < seqVersion; i++ {
			if dao.rd.EXISTS("_DATA_VERSION_DOING_{{.TableName}}_" + u.String(i)) {
				// 遇到仍在处理的版本，跳过更新MAX_VERSION，确保用户获取数据是有序的
				break
			} else {
				// 更新MAX_VERSION，用户可以使用该版本
				dao.rd.SET("_DATA_MAX_VERSION_{{.TableName}}", i)
			}
		}
	}
}
{{ end }}

func (dao *{{.FixedTableName}}Dao) NewQuery() *{{.FixedTableName}}Query {
	return &{{.FixedTableName}}Query{
		dao:            dao,
		validWhere:     "{{.ValidWhere}}",
		sql:            "",
		fields:         "{{.SelectFields}}",
		where:          "",
		extraSql:         "",
		args:           []interface{}{},
		leftJoins:      []string{},
		leftJoinArgs:   []interface{}{},
	}
}

type {{.FixedTableName}}Query struct {
	dao            *{{.FixedTableName}}Dao
	result         *db.QueryResult
	validWhere     string
	sql            string
	fields         string
	where          string
	extraSql       string
	args           []interface{}
	leftJoins      []string
	leftJoinArgs   []interface{}
}

func (query *{{.FixedTableName}}Query) parseFields(fields, table string) string {
	if fields == "" || strings.ContainsRune(fields, '(') || strings.ContainsRune(fields, '` + "`" + `') {
		return fields
	}

	fieldArr := u.SplitWithoutNone(fields, ",")
	for i, field := range fieldArr {
		field = strings.TrimSpace(field)
		as := ""
		if strings.ContainsRune(field, ' ') {
			a := strings.Split(field, " ")
			field = a[0]
			if strings.ToLower(a[len(a)-2]) == "as" && !strings.HasPrefix(a[len(a)-1], "` + "`" + `") {
				a[len(a)-1] = "` + "`" + `" + a[len(a)-1] + "` + "`" + `"
			}
			as = " " + strings.Join(a[1:], " ")
		}
		if table != "" {
			fieldArr[i] = fmt.Sprint("` + "`" + `", table, "` + "`" + `.` + "`" + `", field, "` + "`" + `", as)
		} else {
			fieldArr[i] = fmt.Sprint("` + "`" + `", field, "` + "`" + `", as)
		}
	}
	return strings.Join(fieldArr, ",")
}

func (query *{{.FixedTableName}}Query) parse(tag string) (string, []interface{}) {
	if query.sql != "" {
		return query.sql, query.args
	}

	fields := query.fields
	validWhere := query.validWhere
	if tag == "COUNT" {
		fields = "COUNT(*)"
	}else if tag == "COUNTALL" {
		fields = "COUNT(*)"
		validWhere = ""
	}else if tag == "ALL" {
		validWhere = ""
	}else if tag == "VERSION" {
		validWhere = ""
	}

	leftJoinsStr := ""
	if len(query.leftJoins) > 0 {
		leftJoinsStr = " " + strings.Join(query.leftJoins, " ")
		query.args = append(query.leftJoinArgs, query.args...)
		validWhere = strings.ReplaceAll(validWhere, " AND ", " AND ` + "`" + `{{.TableName}}` + "`" + `.")
	}

	return fmt.Sprint("SELECT ", fields, " FROM ` + "`" + `{{.TableName}}` + "`" + `", leftJoinsStr, " WHERE ", query.where, validWhere, query.extraSql), query.args
}

func (query *{{.FixedTableName}}Query) Sql(sql string, args ...interface{}) *{{.FixedTableName}}Query {
	query.sql = sql
	query.args = args
	return query
}

func (query *{{.FixedTableName}}Query) Fields(fields string) *{{.FixedTableName}}Query {
	query.fields = query.parseFields(fields, "")
	return query
}

func (query *{{.FixedTableName}}Query) AppendFields(fields string) *{{.FixedTableName}}Query {
	if query.fields != "" {
		query.fields += ", "
	}
	query.fields += query.parseFields(fields, "")
	return query
}

func (query *{{.FixedTableName}}Query) fixArgs(args []interface{}) []interface{} {
	if len(args) == 1 {
		t := u.FinalType(reflect.ValueOf(args[0]))
		if t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 {
			return u.ToInterfaceArray(args[0])
		}
	}
	return args
}

func (query *{{.FixedTableName}}Query) Where(where string, args ...interface{}) *{{.FixedTableName}}Query {
	args = query.fixArgs(args)
	query.where = where
	query.args = args
	return query
}

func (query *{{.FixedTableName}}Query) In(field string, values ...interface{}) *{{.FixedTableName}}Query {
	values = query.fixArgs(values)
	query.where = "` + "`" + `"+field+"` + "`" + ` IN "+query.dao.conn.InKeys(len(values))
	query.args = values
	return query
}

func (query *{{.FixedTableName}}Query) And(where string, args ...interface{}) *{{.FixedTableName}}Query {
	args = query.fixArgs(args)
	if query.where != "" {
		query.where += " AND "
	}
	query.where += where
	query.args = append(query.args, args...)
	return query
}

func (query *{{.FixedTableName}}Query) Or(where string, args ...interface{}) *{{.FixedTableName}}Query {
	args = query.fixArgs(args)
	if query.where != "" {
		query.where += " OR "
	}
	query.where += where
	query.args = append(query.args, args...)
	return query
}

func (query *{{.FixedTableName}}Query) AndIn(field string, values ...interface{}) *{{.FixedTableName}}Query {
	values = query.fixArgs(values)
	if query.where != "" {
		query.where += " AND "
	}
	query.where += "` + "`" + `"+field+"` + "`" + ` IN "+query.dao.conn.InKeys(len(values))
	query.args = append(query.args, values...)
	return query
}

func (query *{{.FixedTableName}}Query) OrIn(field string, values ...interface{}) *{{.FixedTableName}}Query {
	values = query.fixArgs(values)
	if query.where != "" {
		query.where += " OR "
	}
	query.where += "` + "`" + `"+field+"` + "`" + ` IN "+query.dao.conn.InKeys(len(values))
	query.args = append(query.args, values...)
	return query
}

func (query *{{.FixedTableName}}Query) OrderBy(orderBy string) *{{.FixedTableName}}Query {
	query.extraSql = " ORDER BY " + orderBy
	return query
}

func (query *{{.FixedTableName}}Query) GroupBy(groupBy string) *{{.FixedTableName}}Query {
	query.extraSql = " GROUP BY " + groupBy
	return query
}

func (query *{{.FixedTableName}}Query) Extra(extraSql string) *{{.FixedTableName}}Query {
	query.extraSql = extraSql
	return query
}

func (query *{{.FixedTableName}}Query) LeftJoin(joinTable, fields, on string, args ...interface{}) *{{.FixedTableName}}Query {
	if !strings.Contains(query.fields, "` + "`" + `{{.TableName}}` + "`" + `.") {
		query.fields = "` + "`" + `{{.TableName}}` + "`" + `."+strings.ReplaceAll(query.fields, "` + "`" + `, ` + "`" + `", "` + "`" + `, ` + "`" + `{{.TableName}}` + "`" + `.` + "`" + `")
	}
	query.fields += ", "+query.parseFields(fields, joinTable)

	query.leftJoins = append(query.leftJoins, fmt.Sprint("LEFT JOIN ` + "`" + `", joinTable, "` + "`" + ` ON ", on))
	query.leftJoinArgs = append(query.leftJoinArgs, args...)
	return query
}

{{range .IndexKeys}}
func (query *{{$.FixedTableName}}Query) By{{.Name}}({{.Params}}) *{{$.FixedTableName}}Query {
	query.Where("{{.Where}}", {{.Args}})
	return query
}
{{ end }}

func (query *{{.FixedTableName}}Query) Query() *{{.FixedTableName}}Query {
	sql, args := query.parse("")
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(sql, args...)
	} else {
		query.result = query.dao.conn.Query(sql, args...)
	}
	return query
}

{{ if .ValidSet }}
func (query *{{.FixedTableName}}Query) QueryWithValid() *{{.FixedTableName}}Query {
	sql, args := query.parse("ALL")
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(sql, args...)
	} else {
		query.result = query.dao.conn.Query(sql, args...)
	}
	return query
}
{{ end }}

func (query *{{.FixedTableName}}Query) Count() int {
	sql, args := query.parse("COUNT")
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(sql, args...)
	} else {
		query.result = query.dao.conn.Query(sql, args...)
	}
	return int(query.result.IntOnR1C1())
}

{{ if .ValidSet }}
func (query *{{.FixedTableName}}Query) CountAll() int {
	sql, args := query.parse("COUNTALL")
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(sql, args...)
	} else {
		query.result = query.dao.conn.Query(sql, args...)
	}
	return int(query.result.IntOnR1C1())
}
{{ end }}

func (query *{{.FixedTableName}}Query) QueryByPage(start, num int) *{{.FixedTableName}}Query {
	sql, args := query.parse("")
	args = append(args, start, num)
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(fmt.Sprint(sql, " LIMIT ?,?"), args...)
	} else {
		query.result = query.dao.conn.Query(fmt.Sprint(sql, " LIMIT ?,?"), args...)
	}
	return query
}

{{ if .ValidSet }}
func (query *{{.FixedTableName}}Query) QueryWithValidByPage(start, num int) *{{.FixedTableName}}Query {
	sql, args := query.parse("ALL")
	args = append(args, start, num)
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(fmt.Sprint(sql, " LIMIT ?,?"), args...)
	} else {
		query.result = query.dao.conn.Query(fmt.Sprint(sql, " LIMIT ?,?"), args...)
	}
	return query
}
{{ end }}

{{ if .HasVersion }}
func (query *{{.FixedTableName}}Query) QueryByVersion(minVersion, maxVersion uint64) uint64 {
	if maxVersion == 0 {
		if query.dao.rd != nil {
			maxVersion = query.dao.rd.GET("_DATA_MAX_VERSION_{{.TableName}}").Uint64()
		} else {
			query.dao.logger.Warning("use version but not configured redis", "dao", "{{.DBName}}", "table", "{{.TableName}}")
		}
		if maxVersion == 0 {
			if query.dao.tx != nil {
				maxVersion = uint64(query.dao.tx.Query("SELECT MAX(` + "`" + `{{.VersionField}}` + "`" + `) FROM ` + "`" + `{{.TableName}}` + "`" + `").IntOnR1C1())
			} else {
				maxVersion = uint64(query.dao.conn.Query("SELECT MAX(` + "`" + `{{.VersionField}}` + "`" + `) FROM ` + "`" + `{{.TableName}}` + "`" + `").IntOnR1C1())
			}
		}
	}
	if minVersion >= maxVersion {
		return maxVersion
	}

	sql, args := query.parse("VERSION")
	args = append(args, minVersion+1, maxVersion)
	if query.validWhere != "" {
		sql = strings.Replace(sql, query.validWhere, "", 1)
	}
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(fmt.Sprint(sql, " AND ` + "`" + `{{.VersionField}}` + "`" + ` BETWEEN ? AND ?"), args...)
	} else {
		query.result = query.dao.conn.Query(fmt.Sprint(sql, " AND ` + "`" + `{{.VersionField}}` + "`" + ` BETWEEN ? AND ?"), args...)
	}
	return maxVersion
}
{{ end }}

func (query *{{.FixedTableName}}Query) Result() *db.QueryResult {
	if query.result == nil {
		query.Query()
	}
	return query.result
}

func (query *{{.FixedTableName}}Query) First() *{{.FixedTableName}}Item {
	list := query.List()
	if len(list) > 0 {
		return &list[0]
	}
	return nil
}

func (query *{{.FixedTableName}}Query) To(out interface{}) {
	query.to(out, "")
}


func (query *{{.FixedTableName}}Query) ToWithoutPrefix(out interface{}, ignorePrefix string) {
	query.to(out, ignorePrefix)
}

func (query *{{.FixedTableName}}Query) to(out interface{}, ignorePrefix string) {
	if query.result == nil {
		query.Query()
	}

	v := reflect.ValueOf(out)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		if v.Type().Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Struct {
			for _, item := range query.List() {
				newItem := reflect.New(v.Type().Elem())
				item.to(newItem.Interface(), ignorePrefix)
				v = reflect.Append(v, newItem.Elem())
			}
			reflect.ValueOf(out).Elem().Set(v)
			return
		} else if v.Type().Kind() == reflect.Struct {
			list := query.List()
			if len(list) > 0 {
				list[0].to(out, ignorePrefix)
			}
			return
		}
	}
	_ = query.result.To(out)
}

func (query *{{.FixedTableName}}Query) ToByFields(out interface{}, fields ...string) {
	if query.result == nil {
		query.Query()
	}

	v := reflect.ValueOf(out)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		if v.Type().Kind() == reflect.Map {
			if v.Type().Elem().Kind() == reflect.Struct || (v.Type().Elem().Kind() == reflect.Ptr && v.Type().Elem().Elem().Kind() == reflect.Struct) {
				for key, item := range query.ListBy(fields...) {
					if v.Type().Elem().Kind() == reflect.Ptr {
						newItem := reflect.New(v.Type().Elem().Elem())
						item.To(newItem.Interface())
						v.SetMapIndex(reflect.ValueOf(key), newItem)
					} else {
						newItem := reflect.New(v.Type().Elem())
						item.To(newItem.Interface())
						v.SetMapIndex(reflect.ValueOf(key), newItem.Elem())
					}
				}
				return
			}
		}
	}
	_ = query.result.To(out)
}

func (query *{{.FixedTableName}}Query) List() []{{.FixedTableName}}Item {
	if query.result == nil {
		query.Query()
	}

	list := make([]{{.FixedTableName}}Item, 0)
	_ = query.result.To(&list)
	for i := range list {
		list[i].dao = query.dao
	}
	return list
}

func (query *{{.FixedTableName}}Query) ListBy(fields ...string) map[string]*{{.FixedTableName}}Item {
	if fields == nil || len(fields) == 0 {
{{ if .PrimaryKey }}
		fields = []string{ {{.PrimaryKey.StringArgs}} }
{{ else }}
		fields = make([]string, 0)
{{ end }}
	}

	if query.result == nil {
		query.Query()
	}

	out := make(map[string]*{{.FixedTableName}}Item)
	list := make([]{{.FixedTableName}}Item, 0)
	_ = query.result.To(&list)
	fieldIndexs := make([]int, len(fields))
	for i, item := range list {
		itemValue := reflect.ValueOf(item)
		itemType := itemValue.Type()
		for i := 0; i < itemType.NumField(); i++ {
			for j := range fieldIndexs {
				if strings.ToLower(fields[j]) == strings.ToLower(itemType.Field(i).Name) {
					fieldIndexs[j] = i
					break
				}
			}
		}

		key := ""
		if len(fieldIndexs) == 1 {
			key = u.String(itemValue.Field(fieldIndexs[0]).Interface())
		} else {
			keys := make([]string, 0)
			for _, i := range fieldIndexs {
				keys = append(keys, u.String(itemValue.Field(fieldIndexs[i]).Interface()))
			}
			key = strings.Join(keys, "_")
		}
		out[key] = &list[i]
	}
	for k := range out {
		out[k].dao = query.dao
	}
	return out
}

func (query *{{.FixedTableName}}Query) LastSql() *string {
	if query.result != nil {
		return query.result.Sql
	}
	return nil
}

func (query *{{.FixedTableName}}Query) LastArgs() []interface{} {
	if query.result != nil {
		return query.result.Args
	}
	return nil
}

func (query *{{.FixedTableName}}Query) LastError() error {
	if query.result != nil {
		return query.result.Error
	}
	return nil
}

{{range .Fields}}{{ if .Options }}
type {{.Type}} string
{{$typ := .Type}}
const (
{{range $k, $v := .Options}}
	{{$k}} {{$typ}} = "{{$v}}"{{ end }}
)
{{ end }}{{ end }}

type {{.FixedTableName}}Item struct {
	dao *{{.FixedTableName}}Dao
{{range .Fields}}
	{{.Name}} {{.Type}}{{ end }}
}

{{range .PointFields}}
func (item *{{$.FixedTableName}}Item) {{.Name}}Value() {{.Type}} {
	if item.{{.Name}} == nil {
		return {{.Default}}
	}
	return *item.{{.Name}}
}

func (item *{{$.FixedTableName}}Item) Set{{.Name}}(value {{.Type}}) {
	item.{{.Name}} = &value
}
{{ end }}

{{ if .PrimaryKey }}

func (item *{{.FixedTableName}}Item) Save() bool {
	if item.dao == nil {
		log.DefaultLogger.Error("save item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		return false
	}
	return item.dao.Replace(item)
}

{{ if .InvalidSet }}
func (item *{{.FixedTableName}}Item) Enable() bool {
	if item.dao == nil {
		log.DefaultLogger.Error("enable item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		return false
	}
	return item.dao.Enable({{.PrimaryKey.ItemArgs}})
}

func (item *{{.FixedTableName}}Item) Disable() bool {
	if item.dao == nil {
		log.DefaultLogger.Error("disable item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		return false
	}
	return item.dao.Disable({{.PrimaryKey.ItemArgs}})
}
{{ else }}
func (item *{{.FixedTableName}}Item) Delete() bool {
	if item.dao == nil {
		log.DefaultLogger.Error("delete item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		return false
	}
	return item.dao.Delete({{.PrimaryKey.ItemArgs}})
}
{{ end }}

{{ end }}

func (item *{{.FixedTableName}}Item) SetByField(field string, value interface{}) {
	fieldValue := reflect.ValueOf(item).Elem().FieldByName(u.GetUpperName(field))
	if fieldValue.IsValid() {
		u.SetValue(fieldValue, reflect.ValueOf(value))
	}
}

func (item *{{.FixedTableName}}Item) ToWithoutPrefix(out interface{}, ignorePrefix string) {
	item.to(out, ignorePrefix)
}

func (item *{{.FixedTableName}}Item) To(out interface{}) {
	item.to(out, "")
}


func (item *{{.FixedTableName}}Item) to(out interface{}, ignorePrefix string) {
	u.Convert(item, out)
	ov := u.FinalValue(reflect.ValueOf(out))
	if ov.Kind() == reflect.Struct {
		iv := reflect.ValueOf(item)

		methods := map[string]reflect.Value{}
		for i := 0; i < iv.NumMethod(); i++ {
			methodName := iv.Type().Method(i).Name
			methodType := iv.Method(i).Type()
			if strings.HasPrefix(methodName, "Get") && methodType.NumIn() == 0 && methodType.NumOut() == 1 {
				methods[methodName[3:]] = iv.Method(i)
			}
		}
		for i := 0; i < ov.NumField(); i++ {
			if ov.Type().Field(i).Name == "dao" {
				continue
			}

			outItemType := ov.Type().Field(i).Type
			if outItemType.Kind() == reflect.Ptr {
				outItemType = outItemType.Elem()
			}
			outKey := ov.Type().Field(i).Name
			outItemTypeName := outItemType.String()
			typedKey := ""
			if strings.HasPrefix(outItemTypeName, "[]") {
				typedKey = outKey+"Array"
			}else{
				typedKey = outKey+u.GetUpperName(outItemTypeName)
			}
			valuedKey := outKey+"Value"
			var method reflect.Value
			if methods[typedKey].IsValid() {
				method = methods[typedKey]
			}else if methods[valuedKey].IsValid(){
				method = methods[valuedKey]
			}else if methods[outKey].IsValid(){
				method = methods[outKey]
			}

			if !method.IsValid() && ignorePrefix != "" {
				outKeyWithPrefix := u.GetUpperName(ignorePrefix)+outKey
				typedKeyWithPrefix := u.GetUpperName(ignorePrefix)+typedKey
				valuedKeyWithPrefix := u.GetUpperName(ignorePrefix)+valuedKey
				if methods[typedKeyWithPrefix].IsValid() {
					method = methods[typedKeyWithPrefix]
				}else if methods[valuedKeyWithPrefix].IsValid(){
					method = methods[valuedKeyWithPrefix]
				}else if methods[outKeyWithPrefix].IsValid(){
					method = methods[outKeyWithPrefix]
				}else{
					if iv.Elem().FieldByName(outKeyWithPrefix).IsValid() {
						u.SetValue(ov.Field(i), iv.Elem().FieldByName(outKeyWithPrefix))
					}
				}
			}

			if method.IsValid() {
				u.SetValue(ov.Field(i), method.Call(nil)[0])
			}
		}
	}
}


`

type FindingDBConfig struct {
	DB string
}

func getDBs(args []string) []string {
	if len(args) > 2 {
		return args[2:]
	} else {
		dbs := make([]string, 0)
		dbExists := map[string]bool{}
		if u.FileExists("env.yml") {
			lines, err := u.ReadFileLines("env.yml")
			if err == nil {
				for _, line := range lines {
					if strings.Contains(line, "mysql://") {
						if strings.ContainsRune(line, '?') {
							line = line[0:strings.IndexByte(line, '?')]
						}
						dbName := line[strings.LastIndexByte(line, '/')+1:]
						if !dbExists[dbName] {
							dbExists[dbName] = true
							dbs = append(dbs, "mysql://"+strings.Split(line, "mysql://")[1])
						}
					}
				}
			}
		}
		files, err := ioutil.ReadDir(".")
		if err == nil {
			for _, file := range files {
				if file.Name()[0] == '.' || !strings.HasSuffix(file.Name(), ".yml") || file.Name() == "env.yml" {
					continue
				}
				lines, err := u.ReadFileLines(file.Name())
				if err == nil {
					for _, line := range lines {
						if strings.Contains(line, "mysql://") {
							if strings.ContainsRune(line, '?') {
								line = line[0:strings.IndexByte(line, '?')]
							}
							dbName := line[strings.LastIndexByte(line, '/')+1:]
							if !dbExists[dbName] {
								dbExists[dbName] = true
								dbs = append(dbs, "mysql://"+strings.Split(line, "mysql://")[1])
							}
						}
					}
				}
			}
		}
		return dbs
	}
}

func main() {
	if len(os.Args) == 1 {
		printUsage()
		return
	}

	conf := DaoConfig{}
	if u.FileExists("dao.yml") {
		_ = u.LoadYaml("dao.yml", &conf)
	}
	if conf.Db == nil {
		conf.Db = getDBs(os.Args)
	}
	if conf.VersionField == "" {
		conf.VersionField = "version"
	}
	if conf.ValidFields == nil {
		conf.ValidFields = []ValidFieldConfig{
			{
				Field:           "isValid",
				Type:            "tinyint",
				ValidOperator:   "!=",
				ValidValue:      "0",
				InvalidOperator: "=",
				InvalidValue:    "0",
			},
			{
				Field:           "isActive",
				Type:            "tinyint",
				ValidOperator:   "!=",
				ValidValue:      "0",
				InvalidOperator: "=",
				InvalidValue:    "0",
			},
			{
				Field:           "deleted",
				Type:            "tinyint",
				ValidOperator:   "=",
				ValidValue:      "0",
				InvalidOperator: "!=",
				InvalidValue:    "0",
			},
			{
				Field:           "status",
				Type:            "tinyint",
				ValidOperator:   "!=",
				ValidValue:      "0",
				InvalidOperator: "=",
				InvalidValue:    "0",
			},
		}
	}

	numberTester := regexp.MustCompile("^[0-9]+$")
	for k, validFieldInfo := range conf.ValidFields {
		if !numberTester.MatchString(validFieldInfo.ValidValue) {
			conf.ValidFields[k].ValidValue = "'" + validFieldInfo.ValidValue + "'"
		}
		if !numberTester.MatchString(validFieldInfo.InvalidValue) {
			conf.ValidFields[k].InvalidValue = "'" + validFieldInfo.InvalidValue + "'"
		}
	}

	op := os.Args[1]
	switch op {
	case "-t":
		if conf.Db == nil {
			fmt.Println("no dsn found")
			printUsage()
			return
		}

		for _, dsn := range conf.Db {
			conn := db.GetDB(dsn, nil)
			r := conn.Query("SHOW TABLES")
			if r.Error != nil {
				fmt.Println("failed to connect to db ", u.Red(r.Error.Error()))
				return
			}
			dbPath := conn.Config.DB + "Dao"
			fmt.Println(conn.Config.DB)
			for _, table := range r.StringsOnC1() {
				if strings.HasPrefix(table, "_") || strings.HasPrefix(table, ".") {
					continue
				}
				dbFile := path.Join(dbPath, "a_"+table+".go")
				exists := u.FileExists(dbFile)
				fmt.Println(" -", table, u.StringIf(exists, u.Green("OK"), u.Dim("Lost")))
			}
		}

	case "-u":
		if conf.Db == nil {
			fmt.Println("no dsn found")
			printUsage()
			return
		}

		for _, dsn := range conf.Db {
			conn := db.GetDB(dsn, nil)
			r := conn.Query("SHOW TABLES")
			if r.Error != nil {
				fmt.Println("failed to connect to db ", u.Red(r.Error.Error()))
				return
			}
			tables := make([]string, 0)
			fixedTables := make([]string, 0)
			for _, table := range r.StringsOnC1() {
				if strings.HasPrefix(table, "_") || strings.HasPrefix(table, ".") {
					continue
				}
				tables = append(tables, table)
				fixedTables = append(fixedTables, strings.ToUpper(table[0:1])+table[1:])
			}

			dbName := conn.Config.DB
			dbPath := dbName + "Dao"
			if !u.FileExists(dbPath) {
				_ = os.Mkdir(dbPath, 0755)
			}

			if files, err := ioutil.ReadDir(dbPath); err == nil {
				for _, file := range files {
					if strings.HasPrefix(file.Name(), "a_") && strings.HasSuffix(file.Name(), ".go") {
						_ = os.Remove(path.Join(dbPath, file.Name()))
					}
				}
			}

			daoData := DaoData{
				DBName:       dbName,
				VersionField: conf.VersionField,
				Tables:       tables,
				FixedTables:  fixedTables,
			}
			dbConfigFile := path.Join(dbPath, "a__config.go")
			err := writeWithTpl(dbConfigFile, configTpl, daoData)
			//if err == nil {
			//	queryFile := path.Join(dbPath, "query.go")
			//	err = writeWithTpl(queryFile, queryTpl, daoData)
			//}
			if err != nil {
				fmt.Println(dbName, u.Red(err.Error()))
			} else {
				fmt.Println(dbName, u.Green("OK"))
			}

			enumTypeExists := map[string]bool{}
			for i, table := range tables {
				fixedTableName := fixedTables[i]
				tableFile := path.Join(dbPath, "a_"+table+".go")

				descs := make([]TableDesc, 0)
				_ = conn.Query("DESC `" + table + "`").To(&descs)

				indexs := make([]TableIndex, 0)
				_ = conn.Query("SHOW INDEX FROM `" + table + "`").To(&indexs)

				tableData := TableData{
					DBName:         dbName,
					TableName:      table,
					FixedTableName: fixedTableName,
					IsAutoId:       false,
					PrimaryKey:     nil,
					UniqueKeys:     make(map[string]*IndexField),
					IndexKeys:      make(map[string]*IndexField),
					Fields:         make([]FieldData, 0),
					PointFields:    make([]FieldData, 0),
					SelectFields:   "",
					ValidField:     "",
					ValidWhere:     "",
					ValidSet:       "",
					InvalidSet:     "",
					VersionField:   "",
					HasVersion:     false,
				}
				fields := make([]string, 0)
				fieldTypesForId := map[string]string{}
				idFields := make([]string, 0)
				idFieldsUpper := make([]string, 0)
				idFieldParams := make([]string, 0)
				idFieldItemArgs := make([]string, 0)
				uniqueFields := map[string][]string{}
				uniqueFieldsUpper := map[string][]string{}
				uniqueFieldParams := map[string][]string{}
				uniqueFieldItemArgs := map[string][]string{}
				indexFields := map[string][]string{}
				indexFieldsUpper := map[string][]string{}
				indexFieldParams := map[string][]string{}
				indexFieldItemArgs := map[string][]string{}

				for _, desc := range descs {
					if strings.Contains(desc.Extra, "auto_increment") {
						tableData.IsAutoId = true
					}

					if desc.Field == conf.VersionField && strings.Contains(desc.Type, "bigint") && strings.Contains(desc.Type, "unsigned") {
						tableData.HasVersion = true
					}

					for _, validFieldInfo := range conf.ValidFields {
						if desc.Field == validFieldInfo.Field && strings.Contains(desc.Type, validFieldInfo.Type) {
							tableData.ValidWhere = " AND `" + validFieldInfo.Field + "`" + validFieldInfo.ValidOperator + validFieldInfo.ValidValue
							tableData.ValidSet = "`" + validFieldInfo.Field + "`" + validFieldInfo.ValidOperator + validFieldInfo.ValidValue
							tableData.InvalidSet = "`" + validFieldInfo.Field + "`" + validFieldInfo.InvalidOperator + validFieldInfo.InvalidValue
						}
					}

					fields = append(fields, desc.Field)

					typ := ""
					defaultValue := "0"
					options := map[string]string{}
					if strings.Contains(desc.Type, "bigint") {
						typ = "int64"
					} else if strings.Contains(desc.Type, "int") {
						typ = "int"
					} else if strings.Contains(desc.Type, "float") {
						typ = "float32"
					} else if strings.Contains(desc.Type, "double") {
						typ = "float64"
					} else if desc.Type == "datetime" {
						typ = "Datetime"
						defaultValue = "\"0000-00-00 00:00:00\""
					} else if desc.Type == "date" {
						typ = "Date"
						defaultValue = "\"0000-00-00\""
					} else if desc.Type == "time" {
						typ = "Time"
						defaultValue = "\"00:00:00\""
					} else if strings.HasPrefix(desc.Type, "enum(") {
						typ = u.GetUpperName(desc.Field)
						if !enumTypeExists[typ] {
							enumTypeExists[typ] = true
							a := u.SplitWithoutNone(desc.Type[5:len(desc.Type)-1], ",")
							for _, v := range a {
								if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
									v = v[1 : len(v)-1]
								}
								options[typ+u.GetUpperName(v)] = v
							}
						}
						defaultValue = "\"\""
					} else {
						typ = "string"
						defaultValue = "\"\""
					}
					if strings.Contains(desc.Type, " unsigned") {
						typ = "u" + typ
					}
					fieldTypesForId[desc.Field] = typ // 用于ID的类型不加指针

					//if desc.Null == "YES" || desc.Default != nil || desc.Extra == "auto_increment" {
					if desc.Null == "YES" || strings.Contains(desc.Extra, "auto_increment") {
						tableData.PointFields = append(tableData.PointFields, FieldData{
							Name:    u.GetUpperName(desc.Field),
							Type:    typ,
							Default: defaultValue,
							Options: options,
						})
						typ = "*" + typ
					}
					tableData.Fields = append(tableData.Fields, FieldData{
						Name:    u.GetUpperName(desc.Field),
						Type:    typ,
						Default: defaultValue,
						Options: options,
					})
					//if desc.Key != "PRI" {
					//	tableData.FieldsWithoutAutoId = append(tableData.FieldsWithoutAutoId, FieldData{
					//		Name: u.GetUpperName(desc.Field),
					//		Type: typ,
					//	})
					//}
				}

				for _, index := range indexs {
					if index.Key_name == "PRIMARY" {
						idFields = append(idFields, index.Column_name)
						idFieldsUpper = append(idFieldsUpper, u.GetUpperName(index.Column_name))
						idFieldParams = append(idFieldParams, index.Column_name+" "+fieldTypesForId[index.Column_name])
						idFieldItemArgs = append(idFieldItemArgs, u.StringIf(tableData.IsAutoId, "*", "")+"item."+u.GetUpperName(index.Column_name))
					} else if index.Non_unique == 0 {
						if uniqueFields[index.Key_name] == nil {
							uniqueFields[index.Key_name] = make([]string, 0)
							uniqueFieldsUpper[index.Key_name] = make([]string, 0)
							uniqueFieldParams[index.Key_name] = make([]string, 0)
							uniqueFieldItemArgs[index.Key_name] = make([]string, 0)
						}
						uniqueFields[index.Key_name] = append(uniqueFields[index.Key_name], index.Column_name)
						uniqueFieldsUpper[index.Key_name] = append(uniqueFieldsUpper[index.Key_name], u.GetUpperName(index.Column_name))
						uniqueFieldParams[index.Key_name] = append(uniqueFieldParams[index.Key_name], index.Column_name+" "+fieldTypesForId[index.Column_name])
						uniqueFieldItemArgs[index.Key_name] = append(uniqueFieldItemArgs[index.Key_name], u.StringIf(tableData.IsAutoId, "*", "")+"item."+u.GetUpperName(index.Column_name))
					} else {
						if indexFields[index.Key_name] == nil {
							indexFields[index.Key_name] = make([]string, 0)
							indexFieldsUpper[index.Key_name] = make([]string, 0)
							indexFieldParams[index.Key_name] = make([]string, 0)
							indexFieldItemArgs[index.Key_name] = make([]string, 0)
						}
						indexFields[index.Key_name] = append(indexFields[index.Key_name], index.Column_name)
						indexFieldsUpper[index.Key_name] = append(indexFieldsUpper[index.Key_name], u.GetUpperName(index.Column_name))
						indexFieldParams[index.Key_name] = append(indexFieldParams[index.Key_name], index.Column_name+" "+fieldTypesForId[index.Column_name])
						indexFieldItemArgs[index.Key_name] = append(indexFieldItemArgs[index.Key_name], u.StringIf(tableData.IsAutoId, "*", "")+"item."+u.GetUpperName(index.Column_name))
					}
				}

				if len(idFields) > 0 {
					tableData.PrimaryKey = &IndexField{
						Name:       strings.Join(idFieldsUpper, ""),
						Where:      "(`" + strings.Join(idFields, "`=? AND `") + "`=?)",
						Args:       strings.Join(idFields, ", "),
						Params:     strings.Join(idFieldParams, ", "),
						ItemArgs:   strings.Join(idFieldItemArgs, ", "),
						StringArgs: "\"" + strings.Join(idFields, "\", \"") + "\"",
					}

					// 将复合主键中的索引添加到 NewQuery().ByXXX
					for i := len(idFields) - 2; i >= 0; i-- {
						name2 := strings.Join(idFieldsUpper[0:i+1], "")
						k2 := "Index_" + name2
						// 唯一索引和普通索引中都不存在时创建
						if tableData.UniqueKeys[k2] == nil && tableData.IndexKeys[k2] == nil {
							tableData.IndexKeys[k2] = &IndexField{
								Name:       name2,
								Where:      "(`" + strings.Join(idFields[0:i+1], "`=? AND `") + "`=?)",
								Args:       strings.Join(idFields[0:i+1], ", "),
								Params:     strings.Join(idFieldParams[0:i+1], ", "),
								ItemArgs:   strings.Join(idFieldItemArgs[0:i+1], ", "),
								StringArgs: "\"" + strings.Join(idFields[0:i+1], "\", \"") + "\"",
							}
						}
					}
				}

				for k, fieldNames := range uniqueFields {
					name1 := strings.Join(uniqueFieldsUpper[k], "")
					k1 := "Unique_" + name1
					if tableData.UniqueKeys[k1] == nil {
						tableData.UniqueKeys[k1] = &IndexField{
							Name:       name1,
							Where:      "(`" + strings.Join(fieldNames, "`=? AND `") + "`=?)",
							Args:       strings.Join(fieldNames, ", "),
							Params:     strings.Join(uniqueFieldParams[k], ", "),
							ItemArgs:   strings.Join(uniqueFieldItemArgs[k], ", "),
							StringArgs: "\"" + strings.Join(fieldNames, "\", \"") + "\"",
						}
					}

					// 将复合唯一索引中的索引添加到 NewQuery().ByXXX
					for i := len(fieldNames) - 2; i >= 0; i-- {
						name2 := strings.Join(uniqueFieldsUpper[k][0:i+1], "")
						k2 := "Index_" + name2
						// 唯一索引和普通索引中都不存在时创建
						if tableData.UniqueKeys[k2] == nil && tableData.IndexKeys[k2] == nil {
							tableData.IndexKeys[k2] = &IndexField{
								Name:       name2,
								Where:      "(`" + strings.Join(fieldNames[0:i+1], "`=? AND `") + "`=?)",
								Args:       strings.Join(fieldNames[0:i+1], ", "),
								Params:     strings.Join(uniqueFieldParams[k][0:i+1], ", "),
								ItemArgs:   strings.Join(uniqueFieldItemArgs[k][0:i+1], ", "),
								StringArgs: "\"" + strings.Join(fieldNames[0:i+1], "\", \"") + "\"",
							}
						}
					}
				}

				// 将其他索引添加到 NewQuery().ByXXX
				for k, fieldNames := range indexFields {
					for i := range fieldNames {
						name := strings.Join(indexFieldsUpper[k][0:i+1], "")
						k2 := "Index_" + name
						// 唯一索引和普通索引中都不存在时创建
						if tableData.UniqueKeys[k2] == nil && tableData.IndexKeys[k2] == nil {
							tableData.IndexKeys[k2] = &IndexField{
								Name:       name,
								Where:      "(`" + strings.Join(fieldNames[0:i+1], "`=? AND `") + "`=?)",
								Args:       strings.Join(fieldNames[0:i+1], ", "),
								Params:     strings.Join(indexFieldParams[k][0:i+1], ", "),
								ItemArgs:   strings.Join(indexFieldItemArgs[k][0:i+1], ", "),
								StringArgs: "\"" + strings.Join(fieldNames[0:i+1], "\", \"") + "\"",
							}
						}
					}
				}
				tableData.SelectFields = "`" + strings.Join(fields, "`, `") + "`"

				err := writeWithTpl(tableFile, tableTpl, tableData)
				if err != nil {
					fmt.Println(" -", table, u.Red(err.Error()))
				} else {
					fmt.Println(" -", table, u.Green("OK"))
				}
			}
		}
	default:
		printUsage()
	}
}

func writeWithTpl(filename, tplContent string, data interface{}) error {
	tpl, err := template.New(filename).Parse(tplContent)
	if err == nil {
		exists := u.FileExists(filename)
		if exists {
			_ = os.Chmod(filename, 0644)
		}

		var fp *os.File
		fp, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0444)
		if err == nil {
			err = tpl.Execute(fp, data)
			_ = fp.Close()
		}

		if exists {
			_ = os.Chmod(filename, 0444)
		}
	}
	return err
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("	dao")
	fmt.Println("	" + u.Cyan("-t  [dsn]") + "	" + u.White("测试数据库连接，并检查已经生成的对象"))
	fmt.Println("	" + u.Cyan("-u  [dsn]") + "	" + u.White("从数据库创建或更新DAO对象"))
	fmt.Println("	dsn	" + u.White("mysql://开头数据库描述，如未指定尝试从env.yml中查找"))
	fmt.Println("")
	fmt.Println("Samples:")
	fmt.Println("	" + u.Cyan("dao -t"))
	fmt.Println("	" + u.Cyan("dao -t mysql://user:password@host:port/db"))
	fmt.Println("	" + u.Cyan("dao -u"))
	fmt.Println("	" + u.Cyan("dao -u mysql://user:password@host:port/db"))
	fmt.Println("")
}
