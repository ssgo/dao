package main

import (
	_ "embed"
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
	Field              string
	Type               string
	ValidOperator      string
	ValidValue         string
	ValidSetOperator   string
	ValidSetValue      string
	InvalidSetOperator string
	InvalidSetValue    string
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

//go:embed a_config.go.tpl
var configTpl string // 当前目录，解析为string类型

//go:embed a_table.go.tpl
var tableTpl string // 当前目录，解析为string类型

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
	DBName          string
	TableName       string
	FixedTableName  string
	IsAutoId        bool
	AutoIdField     string
	AutoIdFieldType string
	PrimaryKey      *IndexField
	UniqueKeys      map[string]*IndexField
	IndexKeys       map[string]*IndexField
	Fields          []FieldData
	PointFields     []FieldData
	//FieldsWithoutAutoId []FieldData
	SelectFields string
	ValidField   string
	ValidWhere   string
	ValidSet     string
	InvalidSet   string
	VersionField string
	HasVersion   bool
}

type FindingDBConfig struct {
	DB string
}

func getDBs(args []string) []string {
	dbs := make([]string, 0)
	filters := make(map[string]bool)
	for i := 2; i < len(args); i++ {
		if strings.Contains(args[i], "://") {
			dbs = append(dbs, args[i])
		} else {
			filters[args[i]] = true
		}
	}
	if len(dbs) == 0 || len(filters) > 0 {
		dbExists := map[string]bool{}

		// 优先查找 env.yml
		if u.FileExists("env.yml") {
			lines, err := u.ReadFileLines("env.yml")
			if err == nil {
				for _, line := range lines {
					if strings.Contains(line, "mysql://") {
						if strings.ContainsRune(line, '?') {
							line = line[0:strings.IndexByte(line, '?')]
						}
						dbName := line[strings.LastIndexByte(line, '/')+1:]
						if len(filters) == 0 || filters[dbName] {
							if !dbExists[dbName] {
								dbExists[dbName] = true
								dbs = append(dbs, "mysql://"+strings.Split(line, "mysql://")[1])
							}
						}
					}
				}
			}
		}

		// 查找更多的
		files, err := ioutil.ReadDir(".")
		if err == nil {
			for _, file := range files {
				if file.Name()[0] == '.' || !strings.HasSuffix(file.Name(), ".yml") || file.Name() == "env.yml" {
					continue
				}
				lines, err := u.ReadFileLines(file.Name())
				if err == nil {
					for _, line := range lines {
						tag := ""
						if strings.Contains(line, "mysql://") {
							tag = "mysql://"
						} else if strings.Contains(line, "postgres://") {
							tag = "postgres://"
						} else if strings.Contains(line, "oci8://") {
							tag = "oci8://"
						} else if strings.Contains(line, "mssql://") {
							tag = "mssql://"
						} else if strings.Contains(line, "sqlite3://") {
							tag = "sqlite3://"
						}
						if strings.Contains(line, tag) {
							if strings.ContainsRune(line, '?') {
								line = line[0:strings.IndexByte(line, '?')]
							}
							dbName := line[strings.LastIndexByte(line, '/')+1:]
							if len(filters) == 0 || filters[dbName] {
								if !dbExists[dbName] {
									dbExists[dbName] = true
									dbs = append(dbs, tag+strings.Split(line, tag)[1])
								}
							}
						}
					}
				}
			}
		}
	}
	return dbs
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
				Field:              "isValid",
				Type:               "tinyint",
				ValidOperator:      "!=",
				ValidValue:         "0",
				ValidSetOperator:   "=",
				ValidSetValue:      "1",
				InvalidSetOperator: "=",
				InvalidSetValue:    "0",
			},
			{
				Field:              "isActive",
				Type:               "tinyint",
				ValidOperator:      "!=",
				ValidValue:         "0",
				ValidSetOperator:   "=",
				ValidSetValue:      "1",
				InvalidSetOperator: "=",
				InvalidSetValue:    "0",
			},
			{
				Field:              "deleted",
				Type:               "tinyint",
				ValidOperator:      "=",
				ValidValue:         "0",
				ValidSetOperator:   "=",
				ValidSetValue:      "0",
				InvalidSetOperator: "=",
				InvalidSetValue:    "1",
			},
			{
				Field:              "status",
				Type:               "tinyint",
				ValidOperator:      "!=",
				ValidValue:         "0",
				ValidSetOperator:   "=",
				ValidSetValue:      "1",
				InvalidSetOperator: "=",
				InvalidSetValue:    "0",
			},
		}
	}

	numberTester := regexp.MustCompile("^[0-9]+$")
	for k, validFieldInfo := range conf.ValidFields {
		if !numberTester.MatchString(validFieldInfo.ValidValue) {
			conf.ValidFields[k].ValidValue = "'" + validFieldInfo.ValidValue + "'"
		}
		if !numberTester.MatchString(validFieldInfo.InvalidSetValue) {
			conf.ValidFields[k].InvalidSetValue = "'" + validFieldInfo.InvalidSetValue + "'"
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
					DBName:          dbName,
					TableName:       table,
					FixedTableName:  fixedTableName,
					IsAutoId:        false,
					AutoIdField:     "",
					AutoIdFieldType: "",
					PrimaryKey:      nil,
					UniqueKeys:      make(map[string]*IndexField),
					IndexKeys:       make(map[string]*IndexField),
					Fields:          make([]FieldData, 0),
					PointFields:     make([]FieldData, 0),
					SelectFields:    "",
					ValidField:      "",
					ValidWhere:      "",
					ValidSet:        "",
					InvalidSet:      "",
					VersionField:    "",
					HasVersion:      false,
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
						tableData.AutoIdField = u.GetUpperName(desc.Field)
					}

					if desc.Field == conf.VersionField && strings.Contains(desc.Type, "bigint") && strings.Contains(desc.Type, "unsigned") {
						tableData.HasVersion = true
					}

					for _, validFieldInfo := range conf.ValidFields {
						if desc.Field == validFieldInfo.Field && strings.Contains(desc.Type, validFieldInfo.Type) {
							tableData.ValidWhere = " AND `" + validFieldInfo.Field + "`" + validFieldInfo.ValidOperator + validFieldInfo.ValidValue
							tableData.ValidSet = "`" + validFieldInfo.Field + "`" + validFieldInfo.ValidSetOperator + validFieldInfo.ValidSetValue
							tableData.InvalidSet = "`" + validFieldInfo.Field + "`" + validFieldInfo.InvalidSetOperator + validFieldInfo.InvalidSetValue
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

					if strings.Contains(desc.Extra, "auto_increment") && tableData.IsAutoId {
						tableData.AutoIdFieldType = typ
					}

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
				//fmt.Println("keys: ", u.JsonP(idFields), u.JsonP(uniqueFields), u.JsonP(indexFields))

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
					for i := len(idFields) - 1; i >= 0; i-- {
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
					for i := len(fieldNames) - 1; i >= 0; i-- {
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
	fmt.Println("	dsn	" + u.White("mysql://、postgres://、oci8://、mssql://、sqlite3:// 等开头数据库描述，如未指定尝试从*.yml中查找"))
	fmt.Println("")
	fmt.Println("Samples:")
	fmt.Println("	" + u.Cyan("dao -t"))
	fmt.Println("	" + u.Cyan("dao -t dbname"))
	fmt.Println("	" + u.Cyan("dao -t mysql://user:password@host:port/db"))
	fmt.Println("	" + u.Cyan("dao -u"))
	fmt.Println("	" + u.Cyan("dao -u dbname"))
	fmt.Println("	" + u.Cyan("dao -u mysql://user:password@host:port/db"))
	fmt.Println("")
}
