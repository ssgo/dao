package main

import (
	_ "embed"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ssgo/dao/dao"
	"github.com/ssgo/db"
	"github.com/ssgo/log"
	"github.com/ssgo/u"
	_ "modernc.org/sqlite"
)

//
//type ValidFieldConfig struct {
//	Field              string
//	Type               string
//	ValidOperator      string
//	ValidValue         string
//	ValidSetOperator   string
//	ValidSetValue      string
//	InvalidSetOperator string
//	InvalidSetValue    string
//}

type DaoConfig struct {
	VersionField string
	ValidFields  []dao.ValidFieldConfig
	Db           []string
}

//type TableDesc struct {
//	Field   string
//	Type    string
//	Null    string
//	Key     string
//	Default *string
//	Extra   string
//	After   string
//}
//
//type TableIndex struct {
//	Non_unique   int
//	Key_name     string
//	Seq_in_index int
//	Column_name  string
//}
//
//type DaoData struct {
//	DBName       string
//	VersionField string
//	Tables       []string
//	FixedTables  []string
//}
//
////go:embed a_config.go.tpl
//var configTpl string // 当前目录，解析为string类型
//
////go:embed dao/a_table.go.tpl
//var tableTpl string // 当前目录，解析为string类型
//
////go:embed dao/a_er.html
//var erTpl string // 当前目录，解析为string类型
//
////type AA string
////
////const (
////	A1 AA = "111"
////	A2 AA = "222"
////)
//
//type FieldData struct {
//	Name    string
//	Type    string
//	Default string
//	Options map[string]string
//}
//
//type IndexField struct {
//	Name       string
//	Where      string
//	Args       string
//	Params     string
//	ItemArgs   string
//	StringArgs string
//}
//
//type TableData struct {
//	DBName          string
//	TableName       string
//	FixedTableName  string
//	IsAutoId        bool
//	AutoIdField     string
//	AutoIdFieldType string
//	PrimaryKey      *IndexField
//	UniqueKeys      map[string]*IndexField
//	IndexKeys       map[string]*IndexField
//	Fields          []FieldData
//	PointFields     []FieldData
//	//FieldsWithoutAutoId []FieldData
//	SelectFields          string
//	ValidField            string
//	ValidWhere            string
//	ValidSet              string
//	InvalidSet            string
//	VersionField          string
//	HasVersion            bool
//	AutoGenerated         []string
//	AutoGeneratedOnUpdate []string
//}
//
//type FindingDBConfig struct {
//	DB string
//}

func getDBs(args []string) []string {
	dbs := make([]string, 0)
	filters := make(map[string]bool)
	if args != nil {
		for i := 0; i < len(args); i++ {
			if strings.Contains(args[i], "://") {
				dbs = append(dbs, args[i])
			} else {
				filters[args[i]] = true
			}
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
		files, err := u.ReadDir(".")
		if err == nil {
			for _, file := range files {
				if file.Name[0] == '.' || !strings.HasSuffix(file.Name, ".yml") || file.Name == "env.yml" {
					continue
				}
				lines, err := u.ReadFileLines(file.Name)
				if err == nil {
					for _, line := range lines {
						tag := ""
						if strings.Contains(line, "mysql://") {
							tag = "mysql://"
						} else if strings.Contains(line, "postgres://") {
							tag = "postgres://"
						} else if strings.Contains(line, "oci8://") {
							tag = "oci8://"
						} else if strings.Contains(line, "sqlserver://") {
							tag = "sqlserver://"
						} else if strings.Contains(line, "sqlite3://") {
							tag = "sqlite3://"
						} else if strings.Contains(line, "sqlite://") {
							tag = "sqlite://"
						} else {
							continue
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

//func fixParamName(in string) string {
//	switch in {
//	case "type":
//		return "typ"
//	}
//	return in
//}
//
//func fixJoinParams(elems []string, sep string) string {
//	a := make([]string, len(elems))
//	for i := len(elems) - 1; i >= 0; i-- {
//		a[i] = fixParamName(elems[i])
//	}
//	return strings.Join(a, sep)
//}

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
		if os.Args[1] == "-i" && len(os.Args) > 3 {
			conf.Db = getDBs(os.Args[3:])
		} else if len(os.Args) > 2 {
			conf.Db = getDBs(os.Args[2:])
		} else {
			conf.Db = getDBs(nil)
		}
	}
	if conf.VersionField == "" {
		conf.VersionField = dao.DefaultVersionField
	}

	if conf.ValidFields == nil {
		conf.ValidFields = dao.DefaultValidFields
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
			_ = dao.MakeDaoFromDBWithOption(conn, conf.VersionField, conf.ValidFields, nil)
		}

	case "-c":
		erInFile := "er.txt"
		if len(os.Args) > 2 {
			erInFile = os.Args[2]
		}
		dbName := strings.SplitN(filepath.Base(erInFile), ".", 2)[0]
		if len(os.Args) > 3 {
			dbName = os.Args[3]
		}
		desc := u.ReadFileN(erInFile)
		_ = dao.MakeDaoFromDescWithOption(desc, dbName, conf.VersionField, conf.ValidFields, nil)

	case "-i":
		if conf.Db == nil || len(conf.Db) == 0 {
			fmt.Println("no dsn found")
			printUsage()
			return
		}

		erInFile := "er.txt"
		if len(os.Args) > 2 {
			erInFile = os.Args[2]
		}
		desc := u.ReadFileN(erInFile)
		conn := db.GetDB(conf.Db[0], log.DefaultLogger)
		_ = dao.MakeDBFromDesc(conn, desc, nil)

	case "-er":
		erInFile := "er.txt"
		erOutFile := "er.html"
		if len(os.Args) > 2 {
			erInFile = os.Args[2]
		}
		dbName := strings.SplitN(filepath.Base(erInFile), ".", 2)[0]
		if len(os.Args) > 3 {
			dbName = os.Args[3]
		}
		if len(os.Args) > 4 {
			erOutFile = os.Args[4]
		} else {
			erOutFile = dbName + ".html"
		}
		desc := u.ReadFileN(erInFile)
		dao.MakeERFile(desc, dbName, erOutFile, nil)

	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("	dao")
	fmt.Println("	" + u.Cyan("-t [dsn]") + "	" + u.White("测试数据库连接，并检查已经生成的对象"))
	fmt.Println("	" + u.Cyan("-u [dsn]") + "	" + u.White("从数据库创建或更新DAO对象"))
	fmt.Println("	" + u.Cyan("-i [erFile] [dsn]") + "	" + u.White("从描述文件导入数据结构"))
	fmt.Println("	" + u.Cyan("-c [erFile] [dbname]") + "	" + u.White("从描述文件创建或更新DAO对象"))
	fmt.Println("	" + u.Cyan("-er [erFile] [dbname] [output file]") + "	" + u.White("从描述文件创建ER图"))
	fmt.Println("	dsn	" + u.White("mysql://、postgres://、oci8://、sqlserver://、sqlite3://、sqlite:// 等开头数据库描述，如未指定尝试从*.yml中查找"))
	fmt.Println("")
	fmt.Println("Samples:")
	fmt.Println("	" + u.Cyan("dao -t"))
	fmt.Println("	" + u.Cyan("dao -t dbname"))
	fmt.Println("	" + u.Cyan("dao -t mysql://user:password@host:port/db"))
	fmt.Println("	" + u.Cyan("dao -u"))
	fmt.Println("	" + u.Cyan("dao -u dbname"))
	fmt.Println("	" + u.Cyan("dao -u mysql://user:password@host:port/db"))
	fmt.Println("	" + u.Cyan("dao -i"))
	fmt.Println("	" + u.Cyan("dao -i er.txt"))
	fmt.Println("	" + u.Cyan("dao -i er.txt dbname"))
	fmt.Println("	" + u.Cyan("dao -i er.txt mysql://user:password@host:port/db"))
	fmt.Println("	" + u.Cyan("dao -c er.txt"))
	fmt.Println("	" + u.Cyan("dao -c er.txt dbname"))
	fmt.Println("	" + u.Cyan("dao -er er.txt"))
	fmt.Println("	" + u.Cyan("dao -er er.txt dbname"))
	fmt.Println("	" + u.Cyan("dao -er er.txt dbname dbname.html"))
	fmt.Println("")
}
