# dao

```shell
    go install github.com/ssgo/dao@latest
```

```shell

Usage:
    dao -t [dsn]                            测试数据库连接，并检查已经生成的对象
    dao -u [dsn]                            从数据库创建或更新DAO对象
    dao -i [erFile] [dsn]                   从描述文件导入数据结构
    dao -c [erFile] [dbname]                从描述文件创建或更新DAO对象
    dao -er [erFile] [dbname] [output file] 从描述文件创建ER图
    [dsn] 以 mysql://、postgres://、oci8://、sqlserver://、sqlite3:// 等开头数据库描述，如未指定尝试从*.yml中查找

Samples:
    dao -t
    dao -t dbname
    dao -t mysql://user:password@host:port/db
    dao -u
    dao -u dbname
    dao -u mysql://user:password@host:port/db
    dao -i
    dao -i er.txt
    dao -i er.txt dbname
    dao -i er.txt mysql://user:password@host:port/db
    dao -c er.txt
    dao -c er.txt dbname
    dao -er er.txt
    dao -er er.txt dbname
    dao -er er.txt dbname dbname.html

```


```
// Account 账号

User                // 用户
id c12 PK           // 用户ID
phone v20 U         // 手机号
password v80 n      // 密码
salt v50            // 随机密码
name v100 n         // 名称
serverKey v200      // 服务密钥
isValid b           // 是否有效

Device              // 设备
id v30 PK           // 设备ID
userId c12          // 当前用户
salt v50            // 随机密码
secretTime dt       // 密钥生成时间

// Log 日志

LoginLog            // 登录日志
id ubi AI           // 登录ID
way v20             // 登录途径（verifyCode/autoLogin/oneClickLogin）
userId c12 I        // 当前用户
deviceId v30 I      // 设备ID
time dt I           // 登录时间
userAgent v200      // 设备信息
requestId v20       // 请求ID
sessionId v20       // 会话ID
successful b        // 是否成功
message v1024       // 登录处理失败的信息
```

## db.makeDB 缩写对照表

### types

```
c   =>  char
v   =>  varchar
dt  =>  datetime
d   =>  date
tm  =>  time
i   =>  int
ui  =>  int unsigned
ti  =>  tinyint
uti =>  tinyint unsigned
b   =>  tinyint unsigned
bi  =>  bigint
ubi =>  bigint unsigned
f   =>  float
uf  =>  float unsigned
ff  =>  double
uff =>  double unsigned
si  =>  smallint
usi =>  smallint unsigned
mi  =>  middleint
umi =>  middleint unsigned
t   =>  text
bb  =>  blob
```

### indexes

```
PK  =>  PRIMARY KEY NOT NULL
AI  =>  PRIMARY KEY AUTOINCREMENT NOT NULL
I   =>  index
U   =>  unique
TI  =>  fulltext
```

### defaults

```
ct  =>  CURRENT_TIMESTAMP
ctu =>  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
```

### null set

```
n   =>  NULL
nn  =>  NOT NULL
```
