package main

import (
	"fmt"
	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/u"
	"strings"
)

type TableFieldDesc struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default string
	Extra   string
	After   string
}

type TableField struct {
	Name       string
	Type       string
	Index      string
	IndexGroup string
	Default    string
	Comment    string
	Null       string
	Extra      string
	Desc       string
}

type TableStruct struct {
	Name    string
	Comment string
	Fields  []TableField
}

func (field *TableField) Parse() {
	//if field.Index == "autoId" {
	//	field.Type += " unsigned"
	//}

	a := make([]string, 0)
	a = append(a, fmt.Sprintf("`%s` %s", field.Name, field.Type))

	lowerType := strings.ToLower(field.Type)
	if strings.Contains(lowerType, "varchar") || strings.Contains(lowerType, "text") {
		a = append(a, " COLLATE utf8mb4_general_ci")
	}

	//if field.Index == "autoId" {
	//	a = append(a, " AUTO_INCREMENT")
	//	field.Index = "pk"
	//	//a = append(a, " NOT NULL")
	//}

	//if field.Index == "uniqueId" {
	//	field.Index = "pk"
	//	//a = append(a, " NOT NULL")
	//}

	if field.Extra != "" {
		a = append(a, " "+field.Extra)
	}
	a = append(a, " "+field.Null)

	if field.Default != "" {
		if strings.Contains(field.Default, "CURRENT_TIMESTAMP") {
			a = append(a, " DEFAULT "+field.Default)
		} else {
			a = append(a, " DEFAULT '"+field.Default+"'")
		}
	}
	if field.Comment != "" {
		a = append(a, " COMMENT '"+field.Comment+"'")
	}
	field.Desc = strings.Join(a, "")
}

func CheckTable(conn *db.DB, table *TableStruct, logger *log.Logger) error {
	//fields = append(fields, &TableField{Name: "creatorId", Type: "varchar(20)"})
	//fields = append(fields, &TableField{Name: "creatorName", Type: "varchar(30)"})
	//fields = append(fields, &TableField{Name: "createTime", Type: "datetime", Default: "CURRENT_TIMESTAMP"})
	//fields = append(fields, &TableField{Name: "updaterId", Type: "varchar(20)"})
	//fields = append(fields, &TableField{Name: "updaterName", Type: "varchar(30)"})
	//fields = append(fields, &TableField{Name: "updateTime", Type: "datetime", Default: "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"})
	//fields = append(fields, &TableField{Name: "isValid", Type: "tinyint unsigned", Default: "1"})
	//fields = append(fields, &TableField{Name: "version", Type: "bigint unsigned", Default: "0", Index: "index"})

	//fieldSets = append(fieldSets, "`creatorId` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL")
	//fieldSets = append(fieldSets, "`creatorName` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL")
	//fieldSets = append(fieldSets, "`createTime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP")
	//fieldSets = append(fieldSets, "`updaterId` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL")
	//fieldSets = append(fieldSets, "`updaterName` varchar(30) COLLATE utf8mb4_general_ci DEFAULT NULL")
	//fieldSets = append(fieldSets, "`updateTime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

	fieldSets := make([]string, 0)
	//fieldSetBy := make(map[string]string)
	pks := make([]string, 0)
	keySets := make([]string, 0)
	keySetBy := make(map[string]string)
	for i, field := range table.Fields {
		field.Parse()
		table.Fields[i] = field
		u.Id8()
		switch field.Index {
		case "pk":
			pks = append(pks, field.Name)
		case "unique":
			keyName := "uk_" + table.Name + field.Name
			if field.IndexGroup != "" {
				keyName = "uk_" + table.Name + field.IndexGroup
			}
			if keySetBy[keyName] != "" {
				// 复合索引
				keySetBy[keyName] = strings.Replace(keySetBy[keyName], ") COMMENT", ", `"+field.Name+"`) COMMENT", 1)
			} else {
				keySet := fmt.Sprintf("UNIQUE KEY `%s` (`%s`) COMMENT '%s'", keyName, field.Name, field.Comment)
				keySets = append(keySets, keySet)
				keySetBy[keyName] = keySet
			}
		case "fulltext":
			keyName := "tk_" + table.Name + field.Name
			keySet := fmt.Sprintf("FULLTEXT KEY `%s` (`%s`) COMMENT '%s'", keyName, field.Name, field.Comment)
			keySets = append(keySets, keySet)
			keySetBy[keyName] = keySet
		case "index":
			keyName := "ik_" + table.Name + field.Name
			if field.IndexGroup != "" {
				keyName = "ik_" + table.Name + field.IndexGroup
			}
			if keySetBy[keyName] != "" {
				// 复合索引
				keySetBy[keyName] = strings.Replace(keySetBy[keyName], ") COMMENT", ", `"+field.Name+"`) COMMENT", 1)
			} else {
				keySet := fmt.Sprintf("KEY `%s` (`%s`) COMMENT '%s'", keyName, field.Name, field.Comment)
				keySets = append(keySets, keySet)
				keySetBy[keyName] = keySet
			}
		}

		fieldSets = append(fieldSets, field.Desc)
		//fieldSetBy[field.Name] = field.Desc
	}
	//fmt.Println(u.JsonP(table.Fields))

	var result *db.ExecResult
	tableInfo := conn.Query("SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + conn.Config.DB + "' AND TABLE_NAME='" + table.Name + "'").MapOnR1()
	//fmt.Println(111, "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + conn.Config.DB + "' AND TABLE_NAME='" + table.Name + "'", u.Json(tableInfo), 111)
	oldTableComment := u.String(tableInfo["TABLE_COMMENT"])

	if tableInfo["TABLE_NAME"] != nil && tableInfo["TABLE_NAME"] != "" {
		// 合并字段
		oldFieldList := make([]*TableFieldDesc, 0)
		oldFields := make(map[string]*TableFieldDesc)
		oldIndexes := make(map[string]string)
		oldIndexInfos := make([]struct {
			Key_name    string
			Column_name string
		}, 0)

		oldComments := map[string]string{}
		conn.Query("SELECT column_name, column_comment FROM information_schema.columns WHERE TABLE_SCHEMA='" + conn.Config.DB + "' AND TABLE_NAME='" + table.Name + "'").ToKV(&oldComments)
		//fmt.Println(u.JsonP(oldComments), 111)

		_ = conn.Query("DESC `" + table.Name + "`").To(&oldFieldList)
		_ = conn.Query("SHOW INDEX FROM `" + table.Name + "`").To(&oldIndexInfos)
		for _, indexInfo := range oldIndexInfos {
			if oldIndexes[indexInfo.Key_name] == "" {
				oldIndexes[indexInfo.Key_name] = indexInfo.Column_name
			} else {
				oldIndexes[indexInfo.Key_name] += " " + indexInfo.Column_name
			}
		}

		// 先后顺序
		prevFieldId := ""
		for _, field := range oldFieldList {
			field.After = prevFieldId
			prevFieldId = field.Field
			oldFields[field.Field] = field
		}

		actions := make([]string, 0)
		for keyId := range oldIndexes {
			if keyId != "PRIMARY" && keySetBy[keyId] == "" {
				actions = append(actions, "DROP KEY `"+keyId+"`")
			}
		}
		//fmt.Println("  =>>>>>>>>", oldIndexes, pks)
		if oldIndexes["PRIMARY"] != "" && oldIndexes["PRIMARY"] != strings.Join(pks, " ") {
			actions = append(actions, "DROP PRIMARY KEY")
		}
		//for fieldId, fieldSet := range fieldSetBy {
		newFieldExists := map[string]bool{}
		prevFieldId = ""
		for _, field := range table.Fields {
			newFieldExists[field.Name] = true
			oldField := oldFields[field.Name]
			if oldField == nil {
				actions = append(actions, "ADD COLUMN "+field.Desc)
			} else {
				fixedOldDefault := u.String(oldField.Default)
				if fixedOldDefault == "CURRENT_TIMESTAMP" && strings.Contains(oldField.Extra, "on update CURRENT_TIMESTAMP") {
					fixedOldDefault = "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
				}
				fixedOldNull := "NOT NULL"
				if oldField.Null == "YES" {
					fixedOldNull = "NULL"
				}
				if field.Type != oldField.Type || field.Default != fixedOldDefault || field.Null != fixedOldNull || oldField.After != prevFieldId || oldComments[field.Name] != field.Comment {
					// `t4f34` varchar(100) COLLATE utf8mb4_general_ci COMMENT ''
					// f34, varchar(100), YES, , ,
					//fmt.Println(111111, u.JsonP(field), 1111)
					// 为什么Desc是空？？？？
					after := ""
					if oldField.After != prevFieldId {
						if prevFieldId == "" {
							after = " FIRST"
						} else {
							after = " AFTER `" + prevFieldId + "`"
						}
					}
					actions = append(actions, "CHANGE `"+field.Name+"` "+field.Desc+after)
				}
			}

			prevFieldId = field.Name
		}

		for oldFieldName := range oldFields {
			if newFieldExists[oldFieldName] != true {
				actions = append(actions, "DROP COLUMN `"+oldFieldName+"`")
			}
		}

		if len(pks) > 0 && oldIndexes["PRIMARY"] != strings.Join(pks, " ") {
			actions = append(actions, "ADD PRIMARY KEY(`"+strings.Join(pks, "`,`")+"`)")
		}

		for keyId, keySet := range keySetBy {
			if oldIndexes[keyId] == "" {
				actions = append(actions, "ADD "+keySet)
			}
		}

		//fmt.Println("	=>", table.Comment, "|", oldTableComment )
		if table.Comment != oldTableComment {
			actions = append(actions, "COMMENT '"+table.Comment+"'")
		}

		if len(actions) == 0 {
			// 不需要更新
			return nil
		}

		sql := "ALTER TABLE `" + table.Name + "` \n  " + strings.Join(actions, ",\n  ") + ";"
		fmt.Println(u.Dim("\t" + strings.ReplaceAll(sql, "\n", "\n\t")))
		result = conn.Exec(sql)
	} else {
		// 创建新表
		if len(pks) > 0 {
			fieldSets = append(fieldSets, "PRIMARY KEY (`"+strings.Join(pks, "`,`")+"`)")
		}

		for _, key := range keySets {
			fieldSets = append(fieldSets, key)
		}

		sql := fmt.Sprintf("CREATE TABLE `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='%s';", table.Name, strings.Join(fieldSets, ",\n  "), table.Comment)
		fmt.Println(u.Dim("\t" + strings.ReplaceAll(sql, "\n", "\n\t")))
		result = conn.Exec(sql)
	}

	if result.Error != nil {
		logger.Error(result.Error.Error())
	}

	return result.Error
}
