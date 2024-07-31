package {{.DBName}}

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

func (serve *Serve) Get{{.FixedTableName}}Dao(logger *log.Logger) *{{.FixedTableName}}Dao {
	if serve.conn == nil {
		log.DefaultLogger.Error("no db configured", "dao", "{{.DBName}}", "table", "{{.TableName}}")
		return nil
	}

	conn := serve.conn
	rd := serve.rd
	if logger != nil {
		conn = serve.conn.CopyByLogger(logger)
		if rd != nil {
			rd = serve.rd.CopyByLogger(logger)
		}
	}

	return &{{.FixedTableName}}Dao{
		conn: conn,
		tx: nil,
		rd: rd,
	}
}

func (serve *Serve) Get{{.FixedTableName}}DaoByTransaction(tx *db.Tx, logger *log.Logger) *{{.FixedTableName}}Dao {
	dao := serve.Get{{.FixedTableName}}Dao(logger)
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
	return &{{.FixedTableName}}Item{dao: dao, isNew: true, changes: map[string]any{}}
}

func (dao *{{.FixedTableName}}Dao) Attach(item *{{.FixedTableName}}Item) {
	item.dao = dao
	item.changes = map[string]any{}
}

{{range .UniqueKeys}}
func (dao *{{$.FixedTableName}}Dao) GetBy{{.Name}}({{.Params}}) *{{$.FixedTableName}}Item {
	result := make([]{{$.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT {{$.SelectFields}} FROM `{{$.TableName}}` WHERE {{.Where}}{{$.ValidWhere}}", {{.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT {{$.SelectFields}} FROM `{{$.TableName}}` WHERE {{.Where}}{{$.ValidWhere}}", {{.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		result[0].changes = map[string]any{}
		return &result[0]
	}
	return nil
}
{{ end }}

{{ if .PrimaryKey }}
func (dao *{{.FixedTableName}}Dao) Get({{.PrimaryKey.Params}}) *{{.FixedTableName}}Item {
	result := make([]{{.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT {{.SelectFields}} FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}{{.ValidWhere}}", {{.PrimaryKey.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT {{.SelectFields}} FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}{{.ValidWhere}}", {{.PrimaryKey.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		result[0].changes = map[string]any{}
		return &result[0]
	}
	return nil
}

func (dao *{{.FixedTableName}}Dao) GetWithFields({{.PrimaryKey.Params}}, fields string) *{{.FixedTableName}}Item {
	result := make([]{{.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT "+fields+" FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}{{.ValidWhere}}", {{.PrimaryKey.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT "+fields+" FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}{{.ValidWhere}}", {{.PrimaryKey.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		result[0].changes = map[string]any{}
		return &result[0]
	}
	return nil
}

{{ if .ValidSet }}
func (dao *{{.FixedTableName}}Dao) GetWithInvalid({{.PrimaryKey.Params}}) *{{.FixedTableName}}Item {
	result := make([]{{.FixedTableName}}Item, 0)
	if dao.tx != nil {
		_ = dao.tx.Query("SELECT {{.SelectFields}} FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}}).To(&result)
	} else {
		_ = dao.conn.Query("SELECT {{.SelectFields}} FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}}).To(&result)
	}
	if len(result) > 0 {
		result[0].dao = dao
		result[0].changes = map[string]any{}
		return &result[0]
	}
	return nil
}
{{ end }}

{{ end }}

{{ if .HasVersion }}
    {{ if .IsAutoId }}
func (dao *{{.FixedTableName}}Dao) Insert(item *{{.FixedTableName}}Item) (int64, bool, uint64) {
    {{ else }}
func (dao *{{.FixedTableName}}Dao) Insert(item *{{.FixedTableName}}Item) (bool, uint64) {
    {{ end }}
{{ else }}
    {{ if .IsAutoId }}
func (dao *{{.FixedTableName}}Dao) Insert(item *{{.FixedTableName}}Item) (int64, bool) {
    {{ else }}
func (dao *{{.FixedTableName}}Dao) Insert(item *{{.FixedTableName}}Item) bool {
    {{ end }}
{{ end }}
    data := make(map[string]interface{})
    u.Convert(item, data)

{{ range $index, $field := .AutoGenerated }}
    if data["{{$field}}"] == nil {
        delete(data, "{{$field}}")
    }
{{ end }}
{{ range $index, $field := .AutoGeneratedOnUpdate }}
    delete(data, "{{$field}}")
{{ end }}

{{ if .HasVersion }}
	version := dao.getVersion()
	data["{{.VersionField}}"] = version
{{ end }}

	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Insert("{{.TableName}}", data)
	} else {
		r = dao.conn.Insert("{{.TableName}}", data)
	}
	dao.lastError = r.Error

{{ if .HasVersion }}
	dao.commitVersion(version)
    {{ if .IsAutoId }}
	return r.Id(), r.Error == nil && r.Changes() > 0, version
    {{ else }}
	return r.Error == nil && r.Changes() > 0, version
    {{ end }}
{{ else }}
    {{ if .IsAutoId }}
	return r.Id(), r.Error == nil && r.Changes() > 0
    {{ else }}
	return r.Error == nil && r.Changes() > 0
    {{ end }}
{{ end }}

}

{{ if .HasVersion }}
func (dao *{{.FixedTableName}}Dao) Replace(item *{{.FixedTableName}}Item) (bool, uint64) {
{{ else }}
func (dao *{{.FixedTableName}}Dao) Replace(item *{{.FixedTableName}}Item) bool {
{{ end }}
    data := make(map[string]interface{})
    u.Convert(item, data)

{{ range $index, $field := .AutoGenerated }}
    if data["{{$field}}"] == nil {
        delete(data, "{{$field}}")
    }
{{ end }}
{{ range $index, $field := .AutoGeneratedOnUpdate }}
    delete(data, "{{$field}}")
{{ end }}

{{ if .HasVersion }}
	version := dao.getVersion()
	data["{{.VersionField}}"] = version
{{ end }}

	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Replace("{{.TableName}}", data)
	} else {
		r = dao.conn.Replace("{{.TableName}}", data)
	}

{{ if .HasVersion }}
	dao.commitVersion(version)
	return r.Error == nil && r.Changes() > 0, version
{{ else }}
	return r.Error == nil && r.Changes() > 0
{{ end }}
}

{{ if .PrimaryKey }}

{{ if .HasVersion }}
func (dao *{{.FixedTableName}}Dao) Update(data interface{}, {{.PrimaryKey.Params}}) (bool, uint64) {
{{ else }}
func (dao *{{.FixedTableName}}Dao) Update(data interface{}, {{.PrimaryKey.Params}}) bool {
{{ end }}
	updateData, ok := data.(map[string]interface{})
	if !ok {
		updateData = make(map[string]interface{})
		u.Convert(data, updateData)
	}

{{ range $index, $field := .AutoGenerated }}
    if updateData["{{$field}}"] == nil {
        delete(updateData, "{{$field}}")
    }
{{ end }}
{{ range $index, $field := .AutoGeneratedOnUpdate }}
    delete(updateData, "{{$field}}")
{{ end }}

{{ if .HasVersion }}
	version := dao.getVersion()
	updateData["{{.VersionField}}"] = version
{{ end }}
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Update("{{.TableName}}", updateData, "{{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Update("{{.TableName}}", updateData, "{{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
	dao.lastError = r.Error

{{ if .HasVersion }}
	dao.commitVersion(version)
	return r.Error == nil && r.Changes() > 0, version
{{ else }}
	return r.Error == nil && r.Changes() > 0
{{ end }}
}

{{ if .InvalidSet }}
{{ if .HasVersion }}
func (dao *{{.FixedTableName}}Dao) Enable({{.PrimaryKey.Params}}) (bool, uint64) {
{{ else }}
func (dao *{{.FixedTableName}}Dao) Enable({{.PrimaryKey.Params}}) bool {
{{ end }}
	var r *db.ExecResult
{{ if .HasVersion }}
	version := dao.getVersion()
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE `{{.TableName}}` SET {{.ValidSet}}, `{{.VersionField}}`=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE `{{.TableName}}` SET {{.ValidSet}}, `{{.VersionField}}`=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	}
	dao.commitVersion(version)
{{ else }}
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE `{{.TableName}}` SET {{.ValidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE `{{.TableName}}` SET {{.ValidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
{{ end }}
	dao.lastError = r.Error
{{ if .HasVersion }}
	return r.Error == nil && r.Changes() > 0, version
{{ else }}
	return r.Error == nil && r.Changes() > 0
{{ end }}
}

{{ if .HasVersion }}
func (dao *{{.FixedTableName}}Dao) Disable({{.PrimaryKey.Params}}) (bool, uint64) {
{{ else }}
func (dao *{{.FixedTableName}}Dao) Disable({{.PrimaryKey.Params}}) bool {
{{ end }}
	var r *db.ExecResult
{{ if .HasVersion }}
	version := dao.getVersion()
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE `{{.TableName}}` SET {{.InvalidSet}}, `{{.VersionField}}`=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE `{{.TableName}}` SET {{.InvalidSet}}, `{{.VersionField}}`=? WHERE {{.PrimaryKey.Where}}", version, {{.PrimaryKey.Args}})
	}
	dao.commitVersion(version)
{{ else }}
	if dao.tx != nil {
		r = dao.tx.Exec("UPDATE `{{.TableName}}` SET {{.InvalidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("UPDATE `{{.TableName}}` SET {{.InvalidSet}} WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
{{ end }}
	dao.lastError = r.Error
{{ if .HasVersion }}
	return r.Error == nil && r.Changes() > 0, version
{{ else }}
	return r.Error == nil && r.Changes() > 0
{{ end }}
}
{{ end }}

func (dao *{{.FixedTableName}}Dao) Delete({{.PrimaryKey.Params}}) bool {
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Exec("DELETE FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	} else {
		r = dao.conn.Exec("DELETE FROM `{{.TableName}}` WHERE {{.PrimaryKey.Where}}", {{.PrimaryKey.Args}})
	}
	dao.lastError = r.Error
	return r.Error == nil && r.Changes() > 0
}

{{ end }}

{{ if .HasVersion }}
func (dao *{{.FixedTableName}}Dao) UpdateBy(data interface{}, where string, args ...interface{}) (bool, uint64) {
{{ else }}
func (dao *{{.FixedTableName}}Dao) UpdateBy(data interface{}, where string, args ...interface{}) bool {
{{ end }}
	updateData, ok := data.(map[string]interface{})
	if !ok {
		updateData = make(map[string]interface{})
		u.Convert(data, updateData)
	}
{{ if .HasVersion }}
	version := dao.getVersion()
	updateData["{{.VersionField}}"] = version
{{ end }}
	var r *db.ExecResult
	if dao.tx != nil {
		r = dao.tx.Update("{{.TableName}}", updateData, where, args...)
	} else {
		r = dao.conn.Update("{{.TableName}}", updateData, where, args...)
	}
	dao.lastError = r.Error
{{ if .HasVersion }}
	dao.commitVersion(version)
	return r.Error == nil && r.Changes() > 0, version
{{ else }}
	return r.Error == nil && r.Changes() > 0
{{ end }}
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

	var r *db.QueryResult
	if dao.tx != nil {
		r = dao.conn.Query("SELECT MAX(`{{.VersionField}}`) FROM `{{.TableName}}`")
	} else {
		r = dao.conn.Query("SELECT MAX(`{{.VersionField}}`) FROM `{{.TableName}}`")
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
		for i := currentMaxVersion; i <= seqVersion; i++ {
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
		extraSql:       "",
		extraArgs:      []interface{}{},
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
	extraArgs      []interface{}
	args           []interface{}
	leftJoins      []string
	leftJoinArgs   []interface{}
}

func (query *{{.FixedTableName}}Query) parseFields(fields, table string) string {
	if fields == "" || strings.ContainsRune(fields, '(') || strings.ContainsRune(fields, '`') {
		return fields
	}

	fieldArr := u.SplitWithoutNone(fields, ",")
	for i, field := range fieldArr {
		field = strings.TrimSpace(field)
		as := ""
		if strings.ContainsRune(field, ' ') {
			a := strings.Split(field, " ")
			field = a[0]
			if strings.ToLower(a[len(a)-2]) == "as" && !strings.HasPrefix(a[len(a)-1], "`") {
				a[len(a)-1] = "`" + a[len(a)-1] + "`"
			}
			as = " " + strings.Join(a[1:], " ")
		}
		if table != "" {
			fieldArr[i] = fmt.Sprint("`", table, "`.`", field, "`", as)
		} else {
			fieldArr[i] = fmt.Sprint("`", field, "`", as)
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
	}else if tag == "COUNT_ALL" {
		fields = "COUNT(*)"
		validWhere = ""
	}else if tag == "ALL" {
		validWhere = ""
	}else if tag == "ALL_VERSION" {
		validWhere = ""
	}else if tag == "VERSION" {
	}

	leftJoinsStr := ""
	if len(query.leftJoins) > 0 {
		leftJoinsStr = " " + strings.Join(query.leftJoins, " ")
		query.args = append(query.leftJoinArgs, query.args...)
		validWhere = strings.ReplaceAll(validWhere, " AND ", " AND `{{.TableName}}`.")
	}

	if query.where == "" && strings.HasPrefix(validWhere, " AND ") {
	    validWhere = validWhere[5:]
	}

    if query.extraArgs != nil {
        query.args = append(query.args, query.extraArgs...)
    }
    whereStr := ""
    if query.where != "" || validWhere != "" {
        whereStr = " WHERE " + query.where + validWhere
    }
	return fmt.Sprint("SELECT ", fields, " FROM `{{.TableName}}`", leftJoinsStr, whereStr, query.extraSql), query.args
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
	if !strings.Contains(field, "`") {
		field = "`"+field+"`"
	}
	values = query.fixArgs(values)
	query.where = field+" IN "+query.dao.conn.InKeys(len(values))
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
	if !strings.Contains(field, "`") {
		field = "`"+field+"`"
	}
	values = query.fixArgs(values)
	if query.where != "" {
		query.where += " AND "
	}
	query.where += field + " IN "+query.dao.conn.InKeys(len(values))
	query.args = append(query.args, values...)
	return query
}

func (query *{{.FixedTableName}}Query) OrIn(field string, values ...interface{}) *{{.FixedTableName}}Query {
	if !strings.Contains(field, "`") {
		field = "`"+field+"`"
	}
	values = query.fixArgs(values)
	if query.where != "" {
		query.where += " OR "
	}
	query.where += field + " IN "+query.dao.conn.InKeys(len(values))
	query.args = append(query.args, values...)
	return query
}

func (query *{{.FixedTableName}}Query) OrderBy(orderBy string) *{{.FixedTableName}}Query {
	query.extraSql += " ORDER BY " + orderBy
	return query
}

func (query *{{.FixedTableName}}Query) GroupBy(groupBy string) *{{.FixedTableName}}Query {
	query.extraSql += " GROUP BY " + groupBy
	return query
}

func (query *{{.FixedTableName}}Query) Limit(start, num uint) *{{.FixedTableName}}Query {
	query.extraSql += " LIMIT ?,?"
	query.extraArgs = append(query.extraArgs, start, num)
	return query
}

func (query *{{.FixedTableName}}Query) Having(where string, args ...interface{}) *{{.FixedTableName}}Query {
	query.extraSql += " HAVING "+where
	query.extraArgs = append(query.extraArgs, args...)
	return query
}

func (query *{{.FixedTableName}}Query) Extra(sql string, args ...interface{}) *{{.FixedTableName}}Query {
	query.extraSql += sql
	query.extraArgs = append(query.extraArgs, args...)
	return query
}

func (query *{{.FixedTableName}}Query) LeftJoin(joinTable, fields, on string, args ...interface{}) *{{.FixedTableName}}Query {
	if !strings.Contains(query.fields, "`{{.TableName}}`.") {
		query.fields = "`{{.TableName}}`."+strings.ReplaceAll(query.fields, "`, `", "`, `{{.TableName}}`.`")
	}
	if fields != "" {
		query.fields += ", "+query.parseFields(fields, joinTable)
	}

	query.leftJoins = append(query.leftJoins, fmt.Sprint("LEFT JOIN `", joinTable, "` ON ", on))
	query.leftJoinArgs = append(query.leftJoinArgs, args...)
	return query
}

{{range .IndexKeys}}
func (query *{{$.FixedTableName}}Query) By{{.Name}}({{.Params}}) *{{$.FixedTableName}}Query {
	query.Where("{{.Where}}", {{.Args}})
	return query
}

func (query *{{$.FixedTableName}}Query) And{{.Name}}({{.Params}}) *{{$.FixedTableName}}Query {
	if query.where != "" {
		query.where += " AND "
	}
	query.where += "{{.Where}}"
	query.args = append(query.args, {{.Args}})
	return query
}

func (query *{{$.FixedTableName}}Query) Or{{.Name}}({{.Params}}) *{{$.FixedTableName}}Query {
	if query.where != "" {
		query.where += " OR "
	}
	query.where += "{{.Where}}"
	query.args = append(query.args, {{.Args}})
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
	sql, args := query.parse("COUNT_ALL")
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(sql, args...)
	} else {
		query.result = query.dao.conn.Query(sql, args...)
	}
	return int(query.result.IntOnR1C1())
}
{{ end }}

func (query *{{.FixedTableName}}Query) QueryByPage(start, num uint) *{{.FixedTableName}}Query {
    query.Limit(start, num)
	return query.Query()
}

{{ if .ValidSet }}
func (query *{{.FixedTableName}}Query) QueryWithValidByPage(start, num uint) *{{.FixedTableName}}Query {
    query.Limit(start, num)
	return query.QueryWithValid()
}
{{ end }}

{{ if .HasVersion }}

{{ if .ValidSet }}
func (query *{{.FixedTableName}}Query) QueryByVersion(minVersion, maxVersion uint64, limit uint, withInvalid bool) (*{{.FixedTableName}}Query, uint64) {
	if minVersion > 0 {
	    withInvalid = true
	}
	parseTag := u.StringIf(withInvalid, "ALL_VERSION", "VERSION")

{{ else }}
func (query *{{.FixedTableName}}Query) QueryByVersion(minVersion, maxVersion uint64, limit uint) (*{{.FixedTableName}}Query, uint64) {
    parseTag := "VERSION"
{{ end }}

	if maxVersion == 0 {
		if query.dao.rd != nil {
			maxVersion = query.dao.rd.GET("_DATA_MAX_VERSION_{{.TableName}}").Uint64()
		} else {
			query.dao.logger.Warning("use version but not configured redis", "dao", "{{.DBName}}", "table", "{{.TableName}}")
		}
		if maxVersion == 0 {
			if query.dao.tx != nil {
				maxVersion = uint64(query.dao.tx.Query("SELECT MAX(`{{.VersionField}}`) FROM `{{.TableName}}`").IntOnR1C1())
			} else {
				maxVersion = uint64(query.dao.conn.Query("SELECT MAX(`{{.VersionField}}`) FROM `{{.TableName}}`").IntOnR1C1())
			}
		}
	}

	query.And("`version` BETWEEN ? AND ?", minVersion+1, maxVersion )
	if limit > 0 {
    	query.OrderBy("`{{.VersionField}}`")
    	query.Limit(0, limit)
	}

	sql, args := query.parse(parseTag)
	if query.dao.tx != nil {
		query.result = query.dao.tx.Query(sql, args...)
	} else {
		query.result = query.dao.conn.Query(sql, args...)
	}

	return query, maxVersion
}
{{ end }}

func (query *{{.FixedTableName}}Query) Result() *db.QueryResult {
	if query.result == nil {
		query.Query()
	}
	return query.result
}

func (query *{{.FixedTableName}}Query) Complete() {
	if query.result != nil {
		query.result.Complete()
	}
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
		list[i].changes = map[string]any{}
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
		out[k].changes = map[string]any{}
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
	isNew bool
	changes map[string]any
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

func (item *{{$.FixedTableName}}Item) Set{{.Name}}Value(value {{.Type}}) {
	item.{{.Name}} = &value
	item.changes["{{.Name}}"] = &value
}
{{ end }}

{{range .Fields}}
func (item *{{$.FixedTableName}}Item) Set{{.Name}}(value {{.Type}}) {
	item.{{.Name}} = value
	item.changes["{{.Name}}"] = value
}
{{ end }}

{{ if .PrimaryKey }}

{{ if .HasVersion }}
func (item *{{.FixedTableName}}Item) Save() (ok bool, version uint64) {
{{ else }}
func (item *{{.FixedTableName}}Item) Save() (ok bool) {
{{ end }}
	if item.dao == nil {
		log.DefaultLogger.Error("save item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		ok = false
		return
	}
	{{ if .IsAutoId }}
	if item.{{.AutoIdField}} == nil {
	    newId, insertOk := item.dao.Insert(item)
	    newIdX := {{.AutoIdFieldType}}(newId)
	    item.{{.AutoIdField}} = &newIdX
	    ok = insertOk
	    return
	}
    {{ end }}
    if item.isNew {
        return item.dao.Insert(item)
    }
    if len(item.changes) == 0 {
	    return item.dao.Replace(item)
    }
    data := item.changes
    item.changes = map[string]any{}
    return item.dao.Update(data, {{.PrimaryKey.ItemArgs}})
}

{{ if .InvalidSet }}
{{ if .HasVersion }}
func (item *{{.FixedTableName}}Item) Enable() (ok bool, version uint64) {
{{ else }}
func (item *{{.FixedTableName}}Item) Enable() (ok bool) {
{{ end }}
	if item.dao == nil {
		log.DefaultLogger.Error("enable item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		ok = false
		return
	}
	return item.dao.Enable({{.PrimaryKey.ItemArgs}})
}

{{ if .HasVersion }}
func (item *{{.FixedTableName}}Item) Disable() (ok bool, version uint64) {
{{ else }}
func (item *{{.FixedTableName}}Item) Disable() (ok bool) {
{{ end }}
	if item.dao == nil {
		log.DefaultLogger.Error("disable item without dao", "dao", "{{.DBName}}", "table", "{{.TableName}}", "item", item)
		ok = false
		return
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
	ii := u.FlatStruct(item)
	oi := u.FlatStruct(out)

	methods := map[string]reflect.Value{}
	for _, method := range ii.Methods {
		methodName := method.Name
		methodValue := ii.MethodValues[methodName]
		if strings.HasPrefix(methodName, "Get") && method.Type.NumIn() == 1 && method.Type.NumOut() == 1 {
			methods[methodName[3:]] = methodValue
		}
	}
	for _, field := range oi.Fields {
		value := oi.Values[field.Name]
		outItemType := field.Type
		if outItemType.Kind() == reflect.Ptr {
			outItemType = outItemType.Elem()
		}
		outKey := field.Name
		outItemTypeName := outItemType.String()
		typedKey := ""
		if strings.HasPrefix(outItemTypeName, "[]") {
			typedKey = outKey + "Array"
		} else {
			typedKey = outKey + u.GetUpperName(outItemTypeName)
		}
		valuedKey := outKey + "Value"
		var method reflect.Value
		if methods[typedKey].IsValid() {
			method = methods[typedKey]
		} else if methods[valuedKey].IsValid() {
			method = methods[valuedKey]
		} else if methods[outKey].IsValid() {
			method = methods[outKey]
		}

		if !method.IsValid() && ignorePrefix != "" {
			outKeyWithPrefix := u.GetUpperName(ignorePrefix) + outKey
			typedKeyWithPrefix := u.GetUpperName(ignorePrefix) + typedKey
			valuedKeyWithPrefix := u.GetUpperName(ignorePrefix) + valuedKey
			if methods[typedKeyWithPrefix].IsValid() {
				method = methods[typedKeyWithPrefix]
			} else if methods[valuedKeyWithPrefix].IsValid() {
				method = methods[valuedKeyWithPrefix]
			} else if methods[outKeyWithPrefix].IsValid() {
				method = methods[outKeyWithPrefix]
			} else {
				if ii.Values[outKeyWithPrefix].IsValid() {
					u.SetValue(value, ii.Values[outKeyWithPrefix])
				}
			}
		}

		if method.IsValid() {
			u.SetValue(value, method.Call(nil)[0])
		}
	}
}
