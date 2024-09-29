package dao

import (
	_ "embed"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/u"
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

type TableDesc struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
	After   string
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

//go:embed a_er.html
var erTpl string // 当前目录，解析为string类型

var DefaultVersionField = "version"
var DefaultValidFields = []ValidFieldConfig{
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
	SelectFields          string
	ValidField            string
	ValidWhere            string
	ValidSet              string
	InvalidSet            string
	VersionField          string
	HasVersion            bool
	AutoGenerated         []string
	AutoGeneratedOnUpdate []string
}

type FindingDBConfig struct {
	DB string
}

func fixParamName(in string) string {
	switch in {
	case "type":
		return "typ"
	}
	return in
}

func fixJoinParams(elems []string, sep string) string {
	a := make([]string, len(elems))
	for i := len(elems) - 1; i >= 0; i-- {
		a[i] = fixParamName(elems[i])
	}
	return strings.Join(a, sep)
}

func MakeDaoFromDB(conn *db.DB, logger *log.Logger) error {
	return MakeDaoFromDBWithOption(conn, DefaultVersionField, DefaultValidFields, logger)
}

func MakeDaoFromDBWithOption(conn *db.DB, versionField string, validFields []ValidFieldConfig, logger *log.Logger) error {
	r := conn.Query("SHOW TABLES")
	if r.Error != nil {
		if logger != nil {
			logger.Error("failed to connect to db", "err", r.Error.Error())
		} else {
			fmt.Println("failed to connect to db", u.Red(r.Error.Error()))
		}
		return r.Error
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

	if files, err := u.ReadDir(dbPath); err == nil {
		for _, file := range files {
			if strings.HasPrefix(file.Name, "a_") && strings.HasSuffix(file.Name, ".go") {
				_ = os.Remove(path.Join(dbPath, file.Name))
			}
		}
	}

	daoData := DaoData{
		DBName:       dbName,
		VersionField: versionField,
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
		if logger != nil {
			logger.Error("failed to make dao config", "dbName", dbName, "err", r.Error.Error())
		} else {
			fmt.Println(dbName, u.Red(err.Error()))
		}
	} else {
		if logger != nil {
			logger.Info("make dao config success", "dbName", dbName)
		} else {
			fmt.Println(dbName, u.Green("OK"))
		}
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
			DBName:                dbName,
			TableName:             table,
			FixedTableName:        fixedTableName,
			IsAutoId:              false,
			AutoIdField:           "",
			AutoIdFieldType:       "",
			PrimaryKey:            nil,
			UniqueKeys:            make(map[string]*IndexField),
			IndexKeys:             make(map[string]*IndexField),
			Fields:                make([]FieldData, 0),
			PointFields:           make([]FieldData, 0),
			SelectFields:          "",
			ValidField:            "",
			ValidWhere:            "",
			ValidSet:              "",
			InvalidSet:            "",
			VersionField:          versionField,
			HasVersion:            false,
			AutoGenerated:         make([]string, 0),
			AutoGeneratedOnUpdate: make([]string, 0),
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
				tableData.AutoGenerated = append(tableData.AutoGenerated, desc.Field)
			}

			// DEFAULT_GENERATED on update CURRENT_TIMESTAMP
			if strings.Contains(desc.Extra, "DEFAULT_GENERATED") {
				if strings.Contains(desc.Extra, "on update") {
					tableData.AutoGeneratedOnUpdate = append(tableData.AutoGeneratedOnUpdate, desc.Field)
				} else {
					tableData.AutoGenerated = append(tableData.AutoGenerated, desc.Field)
				}
			}

			if desc.Field == versionField && strings.Contains(desc.Type, "bigint") && strings.Contains(desc.Type, "unsigned") {
				tableData.HasVersion = true
			}

			for _, validFieldInfo := range validFields {
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
			if strings.Contains(desc.Type, " unsigned") && strings.HasPrefix(typ, "int") {
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
				idFieldParams = append(idFieldParams, fixParamName(index.Column_name)+" "+fieldTypesForId[index.Column_name])
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
				uniqueFieldParams[index.Key_name] = append(uniqueFieldParams[index.Key_name], fixParamName(index.Column_name)+" "+fieldTypesForId[index.Column_name])
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
				indexFieldParams[index.Key_name] = append(indexFieldParams[index.Key_name], fixParamName(index.Column_name)+" "+fieldTypesForId[index.Column_name])
				indexFieldItemArgs[index.Key_name] = append(indexFieldItemArgs[index.Key_name], u.StringIf(tableData.IsAutoId, "*", "")+"item."+u.GetUpperName(index.Column_name))
			}
		}
		//fmt.Println("keys: ", u.JsonP(idFields), u.JsonP(uniqueFields), u.JsonP(indexFields))

		if len(idFields) > 0 {
			tableData.PrimaryKey = &IndexField{
				Name:       strings.Join(idFieldsUpper, ""),
				Where:      "(`" + strings.Join(idFields, "`=? AND `") + "`=?)",
				Args:       fixJoinParams(idFields, ", "),
				Params:     fixJoinParams(idFieldParams, ", "),
				ItemArgs:   strings.Join(idFieldItemArgs, ", "),
				StringArgs: "\"" + fixJoinParams(idFields, "\", \"") + "\"",
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
						Args:       fixJoinParams(idFields[0:i+1], ", "),
						Params:     fixJoinParams(idFieldParams[0:i+1], ", "),
						ItemArgs:   strings.Join(idFieldItemArgs[0:i+1], ", "),
						StringArgs: "\"" + fixJoinParams(idFields[0:i+1], "\", \"") + "\"",
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
					Args:       fixJoinParams(fieldNames, ", "),
					Params:     fixJoinParams(uniqueFieldParams[k], ", "),
					ItemArgs:   strings.Join(uniqueFieldItemArgs[k], ", "),
					StringArgs: "\"" + fixJoinParams(fieldNames, "\", \"") + "\"",
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
						Args:       fixJoinParams(fieldNames[0:i+1], ", "),
						Params:     fixJoinParams(uniqueFieldParams[k][0:i+1], ", "),
						ItemArgs:   strings.Join(uniqueFieldItemArgs[k][0:i+1], ", "),
						StringArgs: "\"" + fixJoinParams(fieldNames[0:i+1], "\", \"") + "\"",
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
						Args:       fixJoinParams(fieldNames[0:i+1], ", "),
						Params:     fixJoinParams(indexFieldParams[k][0:i+1], ", "),
						ItemArgs:   strings.Join(indexFieldItemArgs[k][0:i+1], ", "),
						StringArgs: "\"" + fixJoinParams(fieldNames[0:i+1], "\", \"") + "\"",
					}
				}
			}
		}
		tableData.SelectFields = "`" + strings.Join(fields, "`, `") + "`"

		err := writeWithTpl(tableFile, tableTpl, tableData)
		if err != nil {
			if logger != nil {
				logger.Error("failed to make dao", "tableName", table, "tableFile", tableFile, "err", r.Error.Error())
			} else {
				fmt.Println(" -", table, u.Red(err.Error()))
			}
		} else {
			if logger != nil {
				logger.Info("make dao success", "tableName", table, "tableFile", tableFile)
			} else {
				fmt.Println(" -", table, u.Green("OK"))
			}
		}
	}
	return nil
}

func MakeDaoFromDesc(desc string, dbName string, logger *log.Logger) error {
	return MakeDaoFromDescWithOption(desc, dbName, DefaultVersionField, DefaultValidFields, logger)
}

func MakeDaoFromDescWithOption(desc string, dbName string, versionField string, validFields []ValidFieldConfig, logger *log.Logger) error {
	tablesByGroup := MakeERFromDesc(desc)

	if tablesByGroup != nil {
		tables := make([]string, 0)
		fixedTables := make([]string, 0)
		tableSets := map[string]*TableStruct{}
		for _, g := range tablesByGroup {
			for _, table := range g.Tables {
				if strings.HasPrefix(table.Name, "_") || strings.HasPrefix(table.Name, ".") {
					continue
				}
				tables = append(tables, table.Name)
				tableSets[table.Name] = table
				fixedTables = append(fixedTables, strings.ToUpper(table.Name[0:1])+table.Name[1:])
			}
		}

		dbPath := dbName + "Dao"
		if !u.FileExists(dbPath) {
			_ = os.Mkdir(dbPath, 0755)
		}

		if files, err := u.ReadDir(dbPath); err == nil {
			for _, file := range files {
				if strings.HasPrefix(file.Name, "a_") && strings.HasSuffix(file.Name, ".go") {
					_ = os.Remove(path.Join(dbPath, file.Name))
				}
			}
		}

		daoData := DaoData{
			DBName:       dbName,
			VersionField: versionField,
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
			if logger != nil {
				logger.Error("failed to make dao config", "dbName", dbName, "err", err.Error())
			} else {
				fmt.Println(dbName, u.Red(err.Error()))
			}
		} else {
			if logger != nil {
				logger.Info("make dao config success", "dbName", dbName)
			} else {
				fmt.Println(dbName, u.Green("OK"))
			}
		}

		enumTypeExists := map[string]bool{}
		for i, table := range tables {
			fixedTableName := fixedTables[i]
			tableFile := path.Join(dbPath, "a_"+table+".go")

			//descs := make([]TableDesc, 0)
			//_ = conn.Query("DESC `" + table + "`").To(&descs)
			//
			indexs := make([]TableIndex, 0)
			//_ = conn.Query("SHOW INDEX FROM `" + table + "`").To(&indexs)

			tableData := TableData{
				DBName:                dbName,
				TableName:             table,
				FixedTableName:        fixedTableName,
				IsAutoId:              false,
				AutoIdField:           "",
				AutoIdFieldType:       "",
				PrimaryKey:            nil,
				UniqueKeys:            make(map[string]*IndexField),
				IndexKeys:             make(map[string]*IndexField),
				Fields:                make([]FieldData, 0),
				PointFields:           make([]FieldData, 0),
				SelectFields:          "",
				ValidField:            "",
				ValidWhere:            "",
				ValidSet:              "",
				InvalidSet:            "",
				VersionField:          versionField,
				HasVersion:            false,
				AutoGenerated:         make([]string, 0),
				AutoGeneratedOnUpdate: make([]string, 0),
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

			//for _, desc := range descs {
			tableSet := tableSets[table]
			for _, desc := range tableSet.Fields {
				if desc.Index != "" {
					idx := TableIndex{
						//Non_unique:   0,
						//Key_name:     "",
						//Seq_in_index: 0,
						Column_name: desc.Name,
					}
					switch desc.Index {
					case "pk":
						idx.Key_name = "PRIMARY"
					case "unique":
						idx.Key_name = fmt.Sprint("uk_", table, "_", desc.Name)
						if desc.IndexGroup != "" {
							idx.Key_name = fmt.Sprint("uk_", table, "_", desc.IndexGroup)
						}
					case "fulltext":
						idx.Non_unique = 1
						idx.Key_name = fmt.Sprint("tk_", table, "_", desc.Name)
					case "index":
						idx.Non_unique = 1
						idx.Key_name = fmt.Sprint("ik_", table, "_", desc.Name)
					}
					indexs = append(indexs, idx)
					//idxP := indexs[idx.Key_name]
					//if idxP == nil {
					//	indexs[idx.Key_name] = &idx
					//}else{
					//	idxP.Column_name += " "+idx.Column_name
					//}
				}

				if strings.Contains(desc.Extra, "auto_increment") {
					tableData.IsAutoId = true
					tableData.AutoIdField = u.GetUpperName(desc.Name)
					tableData.AutoGenerated = append(tableData.AutoGenerated, desc.Name)
				}

				// DEFAULT_GENERATED on update CURRENT_TIMESTAMP
				if strings.Contains(desc.Extra, "DEFAULT_GENERATED") {
					if strings.Contains(desc.Extra, "on update") {
						tableData.AutoGeneratedOnUpdate = append(tableData.AutoGeneratedOnUpdate, desc.Name)
					} else {
						tableData.AutoGenerated = append(tableData.AutoGenerated, desc.Name)
					}
				}

				if desc.Name == versionField && strings.Contains(desc.Type, "bigint") && strings.Contains(desc.Type, "unsigned") {
					tableData.HasVersion = true
				}

				for _, validFieldInfo := range validFields {
					if desc.Name == validFieldInfo.Field && strings.Contains(desc.Type, validFieldInfo.Type) {
						tableData.ValidWhere = " AND `" + validFieldInfo.Field + "`" + validFieldInfo.ValidOperator + validFieldInfo.ValidValue
						tableData.ValidSet = "`" + validFieldInfo.Field + "`" + validFieldInfo.ValidSetOperator + validFieldInfo.ValidSetValue
						tableData.InvalidSet = "`" + validFieldInfo.Field + "`" + validFieldInfo.InvalidSetOperator + validFieldInfo.InvalidSetValue
					}
				}

				fields = append(fields, desc.Name)

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
					typ = u.GetUpperName(desc.Name)
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
				if strings.Contains(desc.Type, " unsigned") && strings.HasPrefix(typ, "int") {
					typ = "u" + typ
				}
				fieldTypesForId[desc.Name] = typ // 用于ID的类型不加指针

				if strings.Contains(desc.Extra, "auto_increment") && tableData.IsAutoId {
					tableData.AutoIdFieldType = typ
				}

				//if desc.Null == "YES" || desc.Default != nil || desc.Extra == "auto_increment" {
				if desc.Null == "YES" || strings.Contains(desc.Extra, "auto_increment") {
					tableData.PointFields = append(tableData.PointFields, FieldData{
						Name:    u.GetUpperName(desc.Name),
						Type:    typ,
						Default: defaultValue,
						Options: options,
					})
					typ = "*" + typ
				}
				tableData.Fields = append(tableData.Fields, FieldData{
					Name:    u.GetUpperName(desc.Name),
					Type:    typ,
					Default: defaultValue,
					Options: options,
				})
				//if desc.Key != "PRI" {
				//	tableData.FieldsWithoutAutoId = append(tableData.FieldsWithoutAutoId, FieldData{
				//		Name: u.GetUpperName(desc.Name),
				//		Type: typ,
				//	})
				//}
			}

			for _, index := range indexs {
				if index.Key_name == "PRIMARY" {
					idFields = append(idFields, index.Column_name)
					idFieldsUpper = append(idFieldsUpper, u.GetUpperName(index.Column_name))
					idFieldParams = append(idFieldParams, fixParamName(index.Column_name)+" "+fieldTypesForId[index.Column_name])
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
					uniqueFieldParams[index.Key_name] = append(uniqueFieldParams[index.Key_name], fixParamName(index.Column_name)+" "+fieldTypesForId[index.Column_name])
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
					indexFieldParams[index.Key_name] = append(indexFieldParams[index.Key_name], fixParamName(index.Column_name)+" "+fieldTypesForId[index.Column_name])
					indexFieldItemArgs[index.Key_name] = append(indexFieldItemArgs[index.Key_name], u.StringIf(tableData.IsAutoId, "*", "")+"item."+u.GetUpperName(index.Column_name))
				}
			}
			//fmt.Println("keys: ", u.JsonP(idFields), u.JsonP(uniqueFields), u.JsonP(indexFields))

			if len(idFields) > 0 {
				tableData.PrimaryKey = &IndexField{
					Name:       strings.Join(idFieldsUpper, ""),
					Where:      "(`" + strings.Join(idFields, "`=? AND `") + "`=?)",
					Args:       fixJoinParams(idFields, ", "),
					Params:     fixJoinParams(idFieldParams, ", "),
					ItemArgs:   strings.Join(idFieldItemArgs, ", "),
					StringArgs: "\"" + fixJoinParams(idFields, "\", \"") + "\"",
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
							Args:       fixJoinParams(idFields[0:i+1], ", "),
							Params:     fixJoinParams(idFieldParams[0:i+1], ", "),
							ItemArgs:   strings.Join(idFieldItemArgs[0:i+1], ", "),
							StringArgs: "\"" + fixJoinParams(idFields[0:i+1], "\", \"") + "\"",
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
						Args:       fixJoinParams(fieldNames, ", "),
						Params:     fixJoinParams(uniqueFieldParams[k], ", "),
						ItemArgs:   strings.Join(uniqueFieldItemArgs[k], ", "),
						StringArgs: "\"" + fixJoinParams(fieldNames, "\", \"") + "\"",
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
							Args:       fixJoinParams(fieldNames[0:i+1], ", "),
							Params:     fixJoinParams(uniqueFieldParams[k][0:i+1], ", "),
							ItemArgs:   strings.Join(uniqueFieldItemArgs[k][0:i+1], ", "),
							StringArgs: "\"" + fixJoinParams(fieldNames[0:i+1], "\", \"") + "\"",
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
							Args:       fixJoinParams(fieldNames[0:i+1], ", "),
							Params:     fixJoinParams(indexFieldParams[k][0:i+1], ", "),
							ItemArgs:   strings.Join(indexFieldItemArgs[k][0:i+1], ", "),
							StringArgs: "\"" + fixJoinParams(fieldNames[0:i+1], "\", \"") + "\"",
						}
					}
				}
			}
			tableData.SelectFields = "`" + strings.Join(fields, "`, `") + "`"

			err := writeWithTpl(tableFile, tableTpl, tableData)
			if err != nil {
				if logger != nil {
					logger.Error("failed to make dao", "tableName", table, "tableFile", tableFile, "err", err.Error())
				} else {
					fmt.Println(" -", table, u.Red(err.Error()))
				}
			} else {
				if logger != nil {
					logger.Info("make dao success", "tableName", table, "tableFile", tableFile)
				} else {
					fmt.Println(" -", table, u.Green("OK"))
				}
			}
		}
	} else {
		return errors.New("failed to parse desc")
	}
	return nil
}

func MakeDBFromDesc(conn *db.DB, desc string, logger *log.Logger) error {
	tablesByGroup := MakeERFromDesc(desc)
	//fmt.Println(u.JsonP(tables), ".")

	if tablesByGroup != nil {
		var outErr error
		for _, group := range tablesByGroup {
			if logger == nil {
				fmt.Println(u.Yellow(group.Name))
			}
			for _, table := range group.Tables {
				err := CheckTable(conn, table, logger)
				if err != nil {
					outErr = err
					if logger != nil {
						logger.Error("failed to make table", "tableName", table, "err", err.Error())
					} else {
						fmt.Println("  -", table.Name, table.Comment, u.BRed(err.Error()))
					}
					//} else {
					//	if logger != nil {
					//		logger.Info("make table success", "tableName", table)
					//	} else {
					//		fmt.Println("  -", table.Name, table.Comment, u.BGreen("OK"))
					//	}
				}
			}
		}
		return outErr
	} else {
		return errors.New("failed to parse desc")
	}
}

type ERGroup struct {
	Name   string
	Tables []*TableStruct
}

func MakeERFromDesc(desc string) []*ERGroup {

	//tablesByGroup := map[string]map[string]*TableStruct{}
	tablesByGroup := make([]*ERGroup, 0)
	//tables := map[string]*TableStruct{}
	//	lastGroupName := "default"
	var lastGroup *ERGroup
	lastTableName := ""
	var lastTable *TableStruct
	lastTableComment := ""
	spliter := regexp.MustCompile(`\s+`)
	wnMatcher := regexp.MustCompile(`^([a-zA-Z]+)([0-9]+)$`)
	lines := strings.Split(desc, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		lc := strings.SplitN(line, "//", 2)
		comment := ""
		if len(lc) == 2 {
			line = strings.TrimSpace(lc[0])
			comment = strings.TrimSpace(lc[1])
		}
		if line == "" {
			if comment != "" {
				//lastGroupName = comment
				lastGroup = &ERGroup{
					Name:   comment,
					Tables: make([]*TableStruct, 0),
				}
				tablesByGroup = append(tablesByGroup, lastGroup)
			}
			continue
		}

		a := spliter.Split(line, 10)
		if len(a) == 1 {
			lastTableName = a[0]
			lastTableComment = comment
			lastTable = &TableStruct{
				Name:    lastTableName,
				Comment: lastTableComment,
				Fields:  make([]TableField, 0),
			}
			if lastGroup == nil {
				lastGroup = &ERGroup{
					Name:   "Default",
					Tables: make([]*TableStruct, 0),
				}
				tablesByGroup = append(tablesByGroup, lastGroup)
			}
			lastGroup.Tables = append(lastGroup.Tables, lastTable)
			//if tablesByGroup[lastGroupName] == nil {
			//	tablesByGroup[lastGroupName] = map[string]*TableStruct{}
			//}
			//tablesByGroup[lastGroupName][lastTableName] = lastTable
		} else if lastTableName != "" {
			field := TableField{
				Name:       a[0],
				Type:       "",
				Index:      "",
				IndexGroup: "",
				Default:    "",
				Comment:    comment,
				Null:       "NOT NULL",
				Extra:      "",
				Desc:       "",
			}

			for i := 1; i < len(a); i++ {
				wn := wnMatcher.FindStringSubmatch(a[i])
				tag := a[i]
				size := 0
				if wn != nil {
					tag = wn[1]
					size = u.Int(wn[2])
				}
				switch tag {
				case "PK":
					field.Index = "pk"
					field.Null = "NOT NULL"
				case "I":
					field.Index = "index"
				case "AI":
					field.Extra = "AUTO_INCREMENT"
					field.Index = "pk"
				case "TI":
					field.Index = "fulltext"
				case "U":
					field.Index = "unique"
				case "ct":
					field.Default = "CURRENT_TIMESTAMP"
				case "ctu":
					field.Default = "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
				case "n":
					field.Null = "NULL"
				case "nn":
					field.Null = "NOT NULL"
				case "c":
					field.Type = "char"
				case "v":
					field.Type = "varchar"
				case "dt":
					field.Type = "datetime"
				case "d":
					field.Type = "date"
				case "tm":
					field.Type = "time"
				case "i":
					field.Type = "int"
				case "ui":
					field.Type = "int unsigned"
				case "ti":
					field.Type = "tinyint"
				case "uti":
					field.Type = "tinyint unsigned"
				case "b":
					field.Type = "tinyint unsigned"
				case "bi":
					field.Type = "bigint"
				case "ubi":
					field.Type = "bigint unsigned"
				case "f":
					field.Type = "float"
				case "uf":
					field.Type = "float unsigned"
				case "ff":
					field.Type = "double"
				case "uff":
					field.Type = "double unsigned"
				case "si":
					field.Type = "smallint"
				case "usi":
					field.Type = "smallint unsigned"
				case "mi":
					field.Type = "middleint"
				case "umi":
					field.Type = "middleint unsigned"
				case "t":
					field.Type = "text"
				case "bb":
					field.Type = "blob"
				default:
				}

				if size > 0 {
					switch tag {
					case "I":
						// 索引分组
						field.Index = "index"
						field.IndexGroup = u.String(size)
					case "U":
						// 唯一索引分组
						field.Index = "unique"
						field.IndexGroup = u.String(size)
					default:
						// 带长度的类型
						field.Type += fmt.Sprintf("(%d)", size)
					}
				}
			}
			if lastTable != nil {
				lastTable.Fields = append(lastTable.Fields, field)
			}
		}
	}
	return tablesByGroup
}

func MakeERFile(desc, dbName string, erOutFile string, logger *log.Logger) {
	tablesByGroup := MakeERFromDesc(desc)
	// 创建ER图文件
	tpl := template.New(erOutFile).Funcs(template.FuncMap{
		"short": func(in string) string {
			switch in {
			case "NULL":
				return "n"
			case "NOT NULL":
				return "nn"
			case "AUTO_INCREMENT":
				return "ai"
			case "CURRENT_TIMESTAMP":
				return "ct"
			case "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP":
				return "ctu"
			}
			return in
		},
	})
	var err error
	tpl, err = tpl.Parse(erTpl)
	if err == nil {
		var fp *os.File
		fp, err = os.OpenFile(erOutFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err == nil {
			err = tpl.Execute(fp, map[string]interface{}{
				"title":  dbName,
				"groups": tablesByGroup,
			})
			_ = fp.Close()
		}
	}
	if err != nil {
		if logger != nil {
			logger.Error(err.Error())
		} else {
			fmt.Println(err.Error())
		}
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
