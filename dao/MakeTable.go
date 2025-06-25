package dao

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/u"
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

type TableKeyDesc struct {
	Key_name    string
	Column_name string
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

func (field *TableField) Parse(tableType string) {
	//if field.Index == "autoId" {
	//	field.Type += " unsigned"
	//}

	if strings.HasPrefix(tableType, "sqlite") || tableType == "chai" {
		// sqlite3 不能修改字段，统一使用NULL
		field.Null = "NULL"
		if field.Extra == "AUTOINCREMENT" {
			field.Extra = "PRIMARY KEY AUTOINCREMENT"
			field.Type = "integer"
			field.Null = "NOT NULL"
		}
	}

	a := make([]string, 0)

	if tableType == "mysql" {
		a = append(a, fmt.Sprintf("`%s` %s", field.Name, field.Type))
		lowerType := strings.ToLower(field.Type)
		if strings.Contains(lowerType, "varchar") || strings.Contains(lowerType, "text") {
			a = append(a, " COLLATE utf8mb4_general_ci")
		}
	} else {
		a = append(a, fmt.Sprintf("\"%s\" %s", field.Name, field.Type))
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
		if strings.Contains(field.Default, "CURRENT_TIMESTAMP") || strings.Contains(field.Default, "()") || strings.Contains(field.Default, "SYSTIMESTAMP") {
			a = append(a, " DEFAULT "+field.Default)
		} else {
			a = append(a, " DEFAULT '"+field.Default+"'")
		}
	}
	if strings.HasPrefix(tableType, "sqlite") || tableType == "chai" {
		field.Comment = ""
		field.Type = "numeric"
		// } else if tableType == "mysql" {
	} else {
		if field.Comment != "" {
			a = append(a, " COMMENT '"+field.Comment+"'")
		}
	}
	field.Desc = strings.Join(a, "")
}

// var ddlTableMatcher = regexp.MustCompile("(?is)^\\s*CREATE\\s+TABLE\\s+`?([^)]+)`?\\s*\\(\\s*(.*?)\\s*\\);?\\s*$")
// // var ddlFieldMatcher = regexp.MustCompile("(?s)\\s*[`\\[]?(\\w+)[`\\]]?\\s+\\[?([\\w() ]+)]?\\s*(.*?)(,|$)")
// var ddlFieldMatcher = regexp.MustCompile("(?s)\\s*[`\\[]?([^)]+)[`\\]]?\\s+\\[?([\\w() ]+)]?\\s*(.*?)(,|$)")
// var ddlKeyMatcher = regexp.MustCompile("[`\\[]?([^)]+)[`\\]]?\\s*([,)])")
// var ddlNotNullMatcher = regexp.MustCompile("(?i)\\s+NOT NULL")
// var ddlNullMatcher = regexp.MustCompile("(?i)\\s+NULL")

// // var ddlDefaultMatcher = regexp.MustCompile("(?i)\\s+DEFAULT\\s+(.*?)$")
// var ddlIndexMatcher = regexp.MustCompile("(?is)^\\s*CREATE\\s+([A-Za-z ]+)\\s+`?([^)]+)`?\\s+ON\\s+`?([^)]+)`?\\s*\\(\\s*(.*?)\\s*\\);?\\s*$")
// var ddlIndexFieldMatcher = regexp.MustCompile("[`\\[]?([^)]+)[`\\]]?\\s*(,|$)")

func CheckTable(conn *db.DB, table *TableStruct, logger *log.Logger) error {
	//fmt.Println(u.JsonP(ddlKeyMatcher.FindAllStringSubmatch("(`key`,id, `name` )", 100)), "====================")
	fieldSets := make([]string, 0)
	//fieldSetBy := make(map[string]string)
	pks := make([]string, 0)
	keySets := make([]string, 0)
	keySetBy := make(map[string]string)
	keySetFields := make(map[string]string)
	for i, field := range table.Fields {
		field.Parse(conn.Config.Type)
		table.Fields[i] = field

		// if strings.HasPrefix(conn.Config.Type, "sqlite") {
		// 	if field.Index == "PRIMARY KEY" && field.Extra != "PRIMARY KEY AUTOINCREMENT" {
		// 		// sqlite3 用 unique 代替 pk
		// 		field.Index = "unique"
		// 		field.IndexGroup = "0"
		// 		field.Null = "NULL"
		// 	}
		// }

		switch field.Index {
		case "PRIMARY KEY":
			if strings.HasPrefix(conn.Config.Type, "sqlite") {
				if field.Extra != "PRIMARY KEY AUTOINCREMENT" {
					pks = append(pks, field.Name)
				}
			} else {
				pks = append(pks, field.Name)
			}
		case "unique":
			keyName := fmt.Sprint("uk_", table.Name, "_", field.Name)
			if field.IndexGroup != "" {
				keyName = fmt.Sprint("uk_", table.Name, "_", field.IndexGroup)
			}
			if keySetBy[keyName] != "" {
				keySetFields[keyName] += " " + field.Name
				// 复合索引
				if strings.HasPrefix(conn.Config.Type, "sqlite") || conn.Config.Type == "chai" {
					keySetBy[keyName] = strings.Replace(keySetBy[keyName], ")", ", "+conn.Quote(field.Name)+")", 1)
					// } else if conn.Config.Type == "mysql" {
				} else {
					keySetBy[keyName] = strings.Replace(keySetBy[keyName], ") COMMENT", ", "+conn.Quote(field.Name)+") COMMENT", 1)
				}
			} else {
				keySetFields[keyName] = field.Name
				keySet := ""
				if strings.HasPrefix(conn.Config.Type, "sqlite") || conn.Config.Type == "chai" {
					keySet = fmt.Sprintf("CREATE UNIQUE INDEX \"%s\" ON \"%s\" (\"%s\")", keyName, table.Name, field.Name)
					// } else if conn.Config.Type == "mysql" {
				} else {
					keySet = fmt.Sprintf("UNIQUE KEY "+conn.Quote("%s")+" ("+conn.Quote("%s")+") COMMENT '%s'", keyName, field.Name, field.Comment)
				}
				keySets = append(keySets, keySet)
				keySetBy[keyName] = keySet
			}
		case "fulltext":
			if strings.HasPrefix(conn.Config.Type, "sqlite") || conn.Config.Type == "chai" {
				// } else if conn.Config.Type == "mysql" {
			} else {
				keyName := fmt.Sprint("tk_", table.Name, "_", field.Name)
				keySet := fmt.Sprintf("FULLTEXT KEY "+conn.Quote("%s")+" ("+conn.Quote("%s")+") COMMENT '%s'", keyName, field.Name, field.Comment)
				keySets = append(keySets, keySet)
				keySetBy[keyName] = keySet
			}
		case "index":
			keyName := fmt.Sprint("ik_", table.Name, "_", field.Name)
			if field.IndexGroup != "" {
				keyName = fmt.Sprint("ik_", table.Name, "_", field.IndexGroup)
			}
			if keySetBy[keyName] != "" {
				keySetFields[keyName] += " " + field.Name
				// 复合索引
				if strings.HasPrefix(conn.Config.Type, "sqlite") || conn.Config.Type == "chai" {
					keySetBy[keyName] = strings.Replace(keySetBy[keyName], ")", ", \""+field.Name+"\")", 1)
					// } else if conn.Config.Type == "mysql" {
				} else {
					keySetBy[keyName] = strings.Replace(keySetBy[keyName], ") COMMENT", ", `"+field.Name+"`) COMMENT", 1)
				}
			} else {
				keySetFields[keyName] = field.Name
				keySet := ""
				if strings.HasPrefix(conn.Config.Type, "sqlite") || conn.Config.Type == "chai" {
					keySet = fmt.Sprintf("CREATE INDEX \"%s\" ON \"%s\" (\"%s\")", keyName, table.Name, field.Name)
					// } else if conn.Config.Type == "mysql" {
				} else {
					keySet = fmt.Sprintf("KEY "+conn.Quote("%s")+" ("+conn.Quote("%s")+") COMMENT '%s'", keyName, field.Name, field.Comment)
				}
				keySets = append(keySets, keySet)
				keySetBy[keyName] = keySet
			}
		}

		fieldSets = append(fieldSets, field.Desc)
		//fieldSetBy[field.Name] = field.Desc
	}
	//fmt.Println(u.JsonP(table.Fields))
	//fmt.Println(u.JsonP(keySetBy), 3)
	//fmt.Println(u.JsonP(keySets), 4)

	var result *db.ExecResult
	var tableInfo map[string]interface{}
	if strings.HasPrefix(conn.Config.Type, "sqlite") {
		tableInfo = conn.Query("SELECT \"name\", \"sql\" FROM \"sqlite_master\" WHERE \"type\"='table' AND \"name\"='" + table.Name + "'").MapOnR1()
		tableInfo["comment"] = ""
	} else if conn.Config.Type == "chai" {
		tableInfo = conn.Query("SELECT \"name\", \"sql\" FROM \"__chai_catalog\" WHERE \"type\"='table' AND \"name\"='" + table.Name + "'").MapOnR1()
		tableInfo["comment"] = ""
		// } else if conn.Config.Type == "mysql" {
	} else {
		tableInfo = conn.Query("SELECT TABLE_NAME name, TABLE_COMMENT comment FROM information_schema.TABLES WHERE TABLE_SCHEMA='" + conn.Config.DB + "' AND TABLE_NAME='" + table.Name + "'").MapOnR1()
	}
	oldTableComment := u.String(tableInfo["comment"])

	sqlLog := make([]string, 0)
	if tableInfo["name"] != nil && tableInfo["name"] != "" {
		// 合并字段
		oldFieldList := make([]*TableFieldDesc, 0)
		oldFields := make(map[string]*TableFieldDesc)
		oldIndexes := make(map[string]string)
		oldIndexInfos := make([]*TableKeyDesc, 0)

		oldComments := map[string]string{}
		if strings.HasPrefix(conn.Config.Type, "sqlite") {
			tmpFields := []struct {
				Name       string
				Type       string
				Notnull    bool
				Dflt_value any
				Pk         bool
			}{}
			conn.Query("PRAGMA table_info(" + conn.Quote(table.Name) + ")").To(&tmpFields)
			for _, f := range tmpFields {
				oldFieldList = append(oldFieldList, &TableFieldDesc{
					Field:   f.Name,
					Type:    f.Type,
					Null:    u.StringIf(f.Notnull, "NO", "YES"),
					Key:     u.StringIf(f.Pk, "PRI", ""),
					Default: u.String(f.Dflt_value),
				})
			}

			tmpIndexes := []struct {
				Name    string
				Unique  bool
				Origin  string
				Partial int
			}{}
			conn.Query("PRAGMA index_list(" + conn.Quote(table.Name) + ")").To(&tmpIndexes)
			for _, i := range tmpIndexes {
				tmpIndexInfo := []struct {
					Name  string
					Seqno int
					Cid   int
				}{}
				conn.Query("PRAGMA index_info(" + conn.Quote(i.Name) + ")").To(&tmpIndexInfo)
				if len(tmpIndexInfo) > 0 {
					oldIndexInfos = append(oldIndexInfos, &TableKeyDesc{
						Key_name:    i.Name,
						Column_name: tmpIndexInfo[0].Name,
					})
				}
			}

			// tableM := ddlTableMatcher.FindStringSubmatch(u.String(tableInfo["sql"]))
			// if tableM != nil {
			// 	fmt.Println(u.BCyan(tableM[2]), 110)
			// 	fieldsM := ddlFieldMatcher.FindAllStringSubmatch(tableM[2], 2000)
			// 	fmt.Println(u.BMagenta(u.JsonP(fieldsM)), 111)
			// 	if fieldsM != nil {
			// 		for _, m := range fieldsM {
			// 			if m[1] == "PRIMARY" && m[2] == "KEY" {
			// 				keysM := ddlKeyMatcher.FindAllStringSubmatch(m[3], 20)
			// 				if keysM != nil {
			// 					for _, km := range keysM {
			// 						oldIndexInfos = append(oldIndexInfos, &TableKeyDesc{
			// 							Key_name:    "PRIMARY",
			// 							Column_name: km[1],
			// 						})
			// 					}
			// 				}
			// 			} else {
			// 				nullSet := "NULL"
			// 				//fmt.Println("    =====", m[0], m[1], m[2])
			// 				if ddlNotNullMatcher.MatchString(m[2]) {
			// 					m[2] = ddlNotNullMatcher.ReplaceAllString(m[2], "")
			// 					nullSet = "NOT NULL"
			// 				} else if ddlNullMatcher.MatchString(m[2]) {
			// 					m[2] = ddlNullMatcher.ReplaceAllString(m[2], "")
			// 					nullSet = "NULL"
			// 				}
			// 				//fmt.Println("        =====", m[2], "|", nullSet)

			// 				oldFieldList = append(oldFieldList, &TableFieldDesc{
			// 					Field: m[1],
			// 					Type:  m[2],
			// 					//Null:    u.StringIf(strings.Contains(m[3], "NOT NULL"), "NO", "YES"),
			// 					Null:    u.StringIf(nullSet == "NOT NULL", "NO", "YES"),
			// 					Key:     "",
			// 					Default: "",
			// 					Extra:   "",
			// 					After:   "",
			// 				})
			// 			}
			// 		}
			// 	}
			// 	//fmt.Println(u.JsonP(fieldsM), 222)
			// }

			// // 读取索引信息
			// for _, indexInfo := range conn.Query("SELECT `name`, `sql` FROM `sqlite_master` WHERE `type`='index' AND `tbl_name`='" + table.Name + "'").StringMapResults() {
			// 	//fmt.Println(u.JsonP(indexInfo), 777)
			// 	indexM := ddlIndexMatcher.FindStringSubmatch(indexInfo["sql"])
			// 	if indexM != nil {
			// 		//fmt.Println(u.JsonP(indexM), 666)
			// 		indexFieldM := ddlIndexFieldMatcher.FindAllStringSubmatch(indexM[4], 20)
			// 		//fmt.Println(u.JsonP(indexFieldM), 555)
			// 		if indexFieldM != nil {
			// 			for _, km := range indexFieldM {
			// 				oldIndexInfos = append(oldIndexInfos, &TableKeyDesc{
			// 					Key_name:    indexInfo["name"],
			// 					Column_name: km[1],
			// 				})
			// 			}
			// 		}
			// 	}
			// }

		} else if conn.Config.Type == "chai" {

			// } else if conn.Config.Type == "mysql" {
		} else {
			_ = conn.Query("SELECT column_name, column_comment FROM information_schema.columns WHERE TABLE_SCHEMA='" + conn.Config.DB + "' AND TABLE_NAME='" + table.Name + "'").ToKV(&oldComments)
			_ = conn.Query("DESC " + conn.Quote(table.Name)).To(&oldFieldList)
			_ = conn.Query("SHOW INDEX FROM " + conn.Quote(table.Name)).To(&oldIndexInfos)
		}
		//fmt.Println(u.JsonP(oldComments), 111)

		for _, indexInfo := range oldIndexInfos {
			if oldIndexes[indexInfo.Key_name] == "" {
				oldIndexes[indexInfo.Key_name] = indexInfo.Column_name
			} else {
				oldIndexes[indexInfo.Key_name] += " " + indexInfo.Column_name
			}
		}
		// fmt.Println(u.JsonP(oldFieldList), 1)
		// fmt.Println(u.JsonP(oldIndexInfos), 2)
		// fmt.Println(u.JsonP(oldIndexes), 111)
		//fmt.Println(u.JsonP(keySetFields), 222)
		//fmt.Println(u.JsonP(keySetBy), 333)

		// 先后顺序
		prevFieldId := ""
		for _, field := range oldFieldList {
			if strings.HasPrefix(conn.Config.Type, "sqlite") {
				field.Type = "numeric"
				// } else if conn.Config.Type == "mysql" {
			} else {
				field.After = prevFieldId
			}
			prevFieldId = field.Field
			oldFields[field.Field] = field
		}
		//fmt.Println(111, u.JsonP(oldFields), 111)

		actions := make([]string, 0)
		for keyId := range oldIndexes {
			if keyId != "PRIMARY" && strings.ToLower(keySetFields[keyId]) != strings.ToLower(oldIndexes[keyId]) {
				if strings.HasPrefix(conn.Config.Type, "sqlite") {
					actions = append(actions, "DROP INDEX "+conn.Quote(keyId))
					// } else if conn.Config.Type == "mysql" {
				} else {
					actions = append(actions, "DROP KEY "+conn.Quote(keyId))
				}
			}
		}
		//fmt.Println("  =>>>>>>>>", oldIndexes, pks)
		if oldIndexes["PRIMARY"] != "" && strings.ToLower(oldIndexes["PRIMARY"]) != strings.ToLower(strings.Join(pks, " ")) {
			if strings.HasPrefix(conn.Config.Type, "sqlite") {
				// } else if conn.Config.Type == "mysql" {
			} else {
				actions = append(actions, "DROP PRIMARY KEY")
			}
		}
		//for fieldId, fieldSet := range fieldSetBy {
		newFieldExists := map[string]bool{}
		prevFieldId = ""
		for _, field := range table.Fields {
			newFieldExists[field.Name] = true
			oldField := oldFields[field.Name]
			// 修复部分数据库的特殊性
			if oldField == nil {
				if strings.HasPrefix(conn.Config.Type, "sqlite") {
					actions = append(actions, "ALTER TABLE "+conn.Quote(table.Name)+" ADD COLUMN "+field.Desc)
					// } else if conn.Config.Type == "mysql" {
				} else {
					actions = append(actions, "ADD COLUMN "+field.Desc)
				}
			} else {
				oldField.Type = strings.TrimSpace(strings.ReplaceAll(oldField.Type, " (", "("))
				fixedOldDefault := u.String(oldField.Default)
				if fixedOldDefault == "CURRENT_TIMESTAMP" && strings.Contains(oldField.Extra, "on update CURRENT_TIMESTAMP") {
					fixedOldDefault = "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
				}
				fixedOldNull := "NOT NULL"
				if oldField.Null == "YES" {
					fixedOldNull = "NULL"
				}
				//fmt.Println("  ==", field.Type, "!=", oldField.Type, "||", field.Default, "!=", fixedOldDefault, "||", field.Null, "!=", fixedOldNull, "||", oldField.After, "!=", prevFieldId, "||", oldComments[field.Name], "!=", field.Comment)
				//fmt.Println("  ==", strings.ToLower(field.Type) != strings.ToLower(oldField.Type), strings.ToLower(field.Default) != strings.ToLower(fixedOldDefault), strings.ToLower(field.Null) != strings.ToLower(fixedOldNull), strings.ToLower(oldField.After) != strings.ToLower(prevFieldId), strings.ToLower(oldComments[field.Name]) != strings.ToLower(field.Comment))
				if strings.ToLower(field.Type) != strings.ToLower(oldField.Type) || strings.ToLower(field.Default) != strings.ToLower(fixedOldDefault) || strings.ToLower(field.Null) != strings.ToLower(fixedOldNull) || strings.ToLower(oldField.After) != strings.ToLower(prevFieldId) || strings.ToLower(oldComments[field.Name]) != strings.ToLower(field.Comment) {
					//fmt.Println("    > > > > ", u.JsonP(oldField), 1111)
					// `t4f34` varchar(100) COLLATE utf8mb4_general_ci COMMENT ''
					// f34, varchar(100), YES, , ,
					//fmt.Println(111111, u.JsonP(field), 1111)
					// 为什么Desc是空？？？？
					after := ""

					if strings.HasPrefix(conn.Config.Type, "sqlite") {
						// } else if conn.Config.Type == "mysql" {
					} else {
						if oldField.After != prevFieldId {
							if prevFieldId == "" {
								after = " FIRST"
							} else {
								after = " AFTER " + conn.Quote(prevFieldId)
							}
						}
					}
					//DROP INDEX `uk_config_key`;
					//ALTER TABLE `config` RENAME COLUMN `key` TO `keyOld`;
					//ALTER TABLE `config` ADD COLUMN `key` varchar(30) NULL;
					//UPDATE `config` SET `key`=`keyOld`;
					//ALTER TABLE `config` DROP COLUMN `keyOld`;
					//CREATE INDEX `uk_config_key` ON `config` (`key`);
					if strings.HasPrefix(conn.Config.Type, "sqlite") {
						// 不支持修改字段，所以要先创建然后复制数据再删除

						// 方案一（已放弃）重新创建表实现修改
						//actions = append(actions, fmt.Sprintf("CREATE TABLE `%s_temp` (\n%s\n)", table.Name, strings.Join(fieldSets, ",\n")))
						//actions = append(actions, fmt.Sprintf("INSERT INTO `%s_temp` SELECT * FROM `%s`", table.Name, table.Name))
						//actions = append(actions, fmt.Sprintf("DROP TABLE `%s`", table.Name))
						//actions = append(actions, fmt.Sprintf("ALTER TABLE `%s_temp` RENAME TO `%s`", table.Name, table.Name))
						//INSERT INTO t1_new SELECT foo, bar, baz FROM t1;
						//DROP TABLE t1;
						//ALTER TABLE t1_new RENAME TO t1;

						// 方案二（已放弃）创建新字段复制数据后删除（部分Sqlite不支持DROP COLUMN）
						//redoIndexes := make([]string, 0)
						//for oldIndexName, oldIndex := range oldIndexes {
						//	if u.StringIn(strings.Split(oldIndex, " "), field.Name) {
						//		indexSql := conn.Query("SELECT `sql` FROM `sqlite_master` WHERE `type`='index' AND `name`='" + oldIndexName + "'").StringOnR1C1()
						//		redoIndexes = append(redoIndexes, indexSql)
						//		actions = append(actions, "DROP INDEX `"+oldIndexName+"`")
						//	}
						//}
						//oldPostfix := u.UniqueId()
						//actions = append(actions, "ALTER TABLE `"+table.Name+"` RENAME COLUMN `"+field.Name+"` TO `d_"+field.Name+"_"+oldPostfix+"`")
						//actions = append(actions, "ALTER TABLE `"+table.Name+"` ADD COLUMN "+field.Desc+after)
						//actions = append(actions, "UPDATE `"+table.Name+"` SET `"+field.Name+"`=`d_"+field.Name+"_"+oldPostfix+"`")
						////actions = append(actions, "ALTER TABLE `"+table.Name+"` DROP COLUMN `"+field.Name+"Old`")
						//for _, redoIndex := range redoIndexes {
						//	actions = append(actions, redoIndex)
						//}

						// 方案三 不修改字段类型，Sqlite可以兼容

						//actions = append(actions, "ALTER TABLE `"+table.Name+"` ADD COLUMN "+field.Desc)
						// } else if conn.Config.Type == "mysql" {
					} else {
						actions = append(actions, "CHANGE `"+field.Name+"` "+field.Desc+after)
					}
				}
			}

			if strings.HasPrefix(conn.Config.Type, "sqlite") {
				// } else if conn.Config.Type == "mysql" {
			} else {
				prevFieldId = field.Name
			}
		}

		for oldFieldName := range oldFields {
			if newFieldExists[oldFieldName] != true {
				if strings.HasPrefix(conn.Config.Type, "sqlite") {
					//actions = append(actions, "ALTER TABLE `"+table.Name+"` DROP COLUMN `"+oldFieldName+"`")
					// } else if conn.Config.Type == "mysql" {
				} else {
					actions = append(actions, "DROP COLUMN "+conn.Quote(oldFieldName))
				}
			}
		}

		// sqlite3 不支持添加主键
		if strings.HasPrefix(conn.Config.Type, "sqlite") {
			// } else if conn.Config.Type == "mysql" {
		} else {
			if len(pks) > 0 && strings.ToLower(oldIndexes["PRIMARY"]) != strings.ToLower(strings.Join(pks, " ")) {
				actions = append(actions, "ADD PRIMARY KEY(`"+strings.Join(pks, "`,`")+"`)")
			}
		}

		//fmt.Println(111, u.JsonP(oldIndexes), 222 )
		//fmt.Println(222, u.JsonP(keySetBy), 222 )
		for keyId, keySet := range keySetBy {
			if oldIndexes[keyId] == "" || strings.ToLower(oldIndexes[keyId]) != strings.ToLower(keySetFields[keyId]) {
				if strings.HasPrefix(conn.Config.Type, "sqlite") {
					actions = append(actions, keySet)
					// } else if conn.Config.Type == "mysql" {
				} else {
					actions = append(actions, "ADD "+keySet)
				}
			}
		}

		//fmt.Println("	=>", table.Comment, "|", oldTableComment )
		if strings.HasPrefix(conn.Config.Type, "sqlite") {
			// } else if conn.Config.Type == "mysql" {
		} else {
			if table.Comment != oldTableComment {
				actions = append(actions, "COMMENT '"+table.Comment+"'")
			}
		}

		if len(actions) == 0 {
			// 不需要更新
			return nil
		}

		tx := conn.Begin()
		defer tx.CheckFinished()
		if strings.HasPrefix(conn.Config.Type, "sqlite") {
			for _, action := range actions {
				if logger != nil {
					sqlLog = append(sqlLog, "\t"+strings.ReplaceAll(action, "\n", "\n\t"))
				} else {
					// fmt.Println(u.Dim("\t" + strings.ReplaceAll(action, "\n", "\n\t")))
				}
				result = tx.Exec(action)
				if result.Error != nil {
					break
				}
			}
			// } else if conn.Config.Type == "mysql" {
		} else {
			sql := "ALTER TABLE `" + table.Name + "` " + strings.Join(actions, "\n,") + ";"
			if logger != nil {
				sqlLog = append(sqlLog, "\t"+strings.ReplaceAll(sql, "\n", "\n\t"))
			} else {
				// fmt.Println(u.Dim("\t" + strings.ReplaceAll(sql, "\n", "\n\t")))
			}
			result = tx.Exec(sql)
		}
		if result != nil && result.Error != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	} else {
		// 创建新表
		if len(pks) > 0 {
			// fieldSets = append(fieldSets, "PRIMARY KEY (`"+strings.Join(pks, "`,`")+"`)")
			fieldSets = append(fieldSets, "PRIMARY KEY ("+conn.Quotes(pks)+")")
		}

		indexSets := make([]string, 0) // sqlite3 额外创建索引的sql
		if strings.HasPrefix(conn.Config.Type, "sqlite") {
			for _, indexSql := range keySetBy {
				indexSets = append(indexSets, indexSql)
			}
			// } else if conn.Config.Type == "mysql" {
		} else {
			for _, key := range keySets {
				fieldSets = append(fieldSets, key)
			}
		}

		sql := ""

		if strings.HasPrefix(conn.Config.Type, "sqlite") {
			sql = fmt.Sprintf("CREATE TABLE \"%s\" (\n%s\n);", table.Name, strings.Join(fieldSets, ",\n"))
			// } else if conn.Config.Type == "mysql" {
		} else {
			sql = fmt.Sprintf("CREATE TABLE "+conn.Quote("%s")+" (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='%s';", table.Name, strings.Join(fieldSets, ",\n"), table.Comment)
		}
		tx := conn.Begin()
		defer tx.CheckFinished()
		if logger != nil {
			sqlLog = append(sqlLog, "\t"+strings.ReplaceAll(sql, "\n", "\n\t"))
		} else {
			// fmt.Println(u.Dim("\t" + strings.ReplaceAll(sql, "\n", "\n\t")))
		}
		result = tx.Exec(sql)

		if result.Error == nil {
			if strings.HasPrefix(conn.Config.Type, "sqlite") {
				for _, indexSet := range indexSets {
					//fmt.Println(indexSet)
					if logger != nil {
						sqlLog = append(sqlLog, "\t"+strings.ReplaceAll(indexSet, "\n", "\n\t"))
					} else {
						// fmt.Println(u.Dim("\t" + strings.ReplaceAll(indexSet, "\n", "\n\t")))
					}
					r := tx.Exec(indexSet)
					if r.Error != nil {
						result = r
					}
				}
			}
		}
		if result.Error != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}

	}

	if logger != nil {
		logger.Info("run sql", "sql", strings.Join(sqlLog, "\n"), "size", len(sqlLog), "sss", u.JsonP(sqlLog))
	}

	if result == nil {
		return nil
	}

	if result.Error != nil {
		if logger != nil {
			logger.Error(result.Error.Error())
		} else {
			// fmt.Println(result.Error.Error())
		}
	}

	return result.Error
}

var fieldSpliter = regexp.MustCompile(`\s+`)
var wnMatcher = regexp.MustCompile(`^([a-zA-Z]+)([0-9]+)$`)

// func ParseField(dbType, line string) TableField {
// 	lc := strings.SplitN(line, "//", 2)
// 	comment := ""
// 	if len(lc) == 2 {
// 		line = strings.TrimSpace(lc[0])
// 		comment = strings.TrimSpace(lc[1])
// 	}

// 	a := fieldSpliter.Split(line, 10)
// 	field := TableField{
// 		Name:       a[0],
// 		Type:       "",
// 		Index:      "",
// 		IndexGroup: "",
// 		Default:    "",
// 		Comment:    comment,
// 		Null:       "NULL",
// 		Extra:      "",
// 		Desc:       "",
// 	}

// 	for i := 1; i < len(a); i++ {
// 		wn := wnMatcher.FindStringSubmatch(a[i])
// 		tag := a[i]
// 		size := 0
// 		if wn != nil {
// 			tag = wn[1]
// 			size = u.Int(wn[2])
// 		}
// 		switch tag {
// 		case "PK":
// 			field.Index = "pk"
// 			field.Null = "NOT NULL"
// 		case "I":
// 			field.Index = "index"
// 		case "AI":
// 			field.Extra = "AUTO_INCREMENT"
// 			field.Index = "pk"
// 			field.Null = "NOT NULL"
// 		case "TI":
// 			field.Index = "fulltext"
// 		case "U":
// 			field.Index = "unique"
// 		case "ct":
// 			field.Default = "CURRENT_TIMESTAMP"
// 		case "ctu":
// 			field.Default = "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
// 		case "n":
// 			field.Null = "NULL"
// 		case "nn":
// 			field.Null = "NOT NULL"
// 		case "c":
// 			field.Type = "char"
// 		case "v":
// 			field.Type = "varchar"
// 		case "dt":
// 			field.Type = "datetime(6)"
// 		case "d":
// 			field.Type = "date"
// 		case "tm":
// 			field.Type = "time(6)"
// 		case "i":
// 			field.Type = "int"
// 		case "ui":
// 			field.Type = "int unsigned"
// 		case "ti":
// 			field.Type = "tinyint"
// 		case "uti":
// 			field.Type = "tinyint unsigned"
// 		case "b":
// 			field.Type = "tinyint unsigned"
// 		case "bi":
// 			field.Type = "bigint"
// 		case "ubi":
// 			field.Type = "bigint unsigned"
// 		case "f":
// 			field.Type = "float"
// 		case "uf":
// 			field.Type = "float unsigned"
// 		case "ff":
// 			field.Type = "double"
// 		case "uff":
// 			field.Type = "double unsigned"
// 		case "si":
// 			field.Type = "smallint"
// 		case "usi":
// 			field.Type = "smallint unsigned"
// 		case "mi":
// 			field.Type = "middleint"
// 		case "umi":
// 			field.Type = "middleint unsigned"
// 		case "t":
// 			field.Type = "text"
// 		case "bb":
// 			field.Type = "blob"
// 		default:
// 			field.Type = tag
// 		}

// 		if size > 0 {
// 			switch tag {
// 			case "I":
// 				// 索引分组
// 				field.Index = "index"
// 				field.IndexGroup = u.String(size)
// 			case "U":
// 				// 唯一索引分组
// 				field.Index = "unique"
// 				field.IndexGroup = u.String(size)
// 			default:
// 				// 带长度的类型
// 				field.Type += fmt.Sprintf("(%d)", size)
// 			}
// 		}
// 	}
// 	return field
// }

// func ParseFields(dbType string, lines []string) []TableField {
// 	fields := make([]TableField, 0)
// 	for _, line := range lines {
// 		line = strings.TrimSpace(line)
// 		if line == "" || strings.HasPrefix(line, "//") {
// 			continue
// 		}

// 		field := ParseField(dbType, line)
// 		fields = append(fields, field)
// 	}

// 	return fields
// }
