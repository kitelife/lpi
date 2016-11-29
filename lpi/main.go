package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"

	"github.com/Sirupsen/logrus"
	"github.com/julienschmidt/httprouter"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/yaml.v2"

	"github.com/youngsterxyf/lpi"
)

type appConfT struct {
	Leveldbs map[string][]string `yaml:"leveldbs"`
}

var appConfPath string
var port int
var debug bool
var appConf appConfT
var dbs map[string]*leveldb.DB

func _logRequest(req *http.Request) {
	if debug {
		log.Printf("%s %s?%s remoteAddr=%s, UA=%s", req.Method, req.URL.Path,
			req.URL.RawQuery, req.RemoteAddr, req.UserAgent())
	}
	return
}

func _respJSON(resp http.ResponseWriter, data ...interface{}) {
	respBody := lpi.RespBodyT{
		Code:    lpi.StatusOK,
		Message: lpi.StatusMsgs[lpi.StatusOK],
		Data:    true,
	}
	if len(data) > 0 {
		respBody.Data = data[0]
	}
	resp.Header().Set("Content-Type", lpi.ContentTypeJSON)
	respBodyBytes, _ := json.Marshal(respBody)
	io.WriteString(resp, string(respBodyBytes))
}

func _respJSONError(resp http.ResponseWriter, code int, msgs ...string) {
	respBody := lpi.RespBodyT{
		Code:    code,
		Message: lpi.StatusMsgs[code],
		Data:    false,
	}
	if len(msgs) > 0 {
		respBody.Message = msgs[0]
	}
	resp.Header().Set("Content-Type", lpi.ContentTypeJSON)
	respBodyBytes, _ := json.Marshal(respBody)
	io.WriteString(resp, string(respBodyBytes))
}

// leveldbPut 对leveldb执行put操作
func leveldbPut(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// 支持batch模式
	/*
		请求体格式：
		{
			"keyprefix": "",
			"kv": [
				{"k": "", "v": ""},
				...
			]
		}
	*/
	go _logRequest(req)
	//
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, "请求体读取失败")
		return
	}
	var reqBody lpi.PutReqBodyT
	//
	if err = json.Unmarshal(body, &reqBody); err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, "请求体解析失败")
		return
	}
	//
	DB, err := getDBByKeyPrefix(reqBody.KeyPrefix)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, err.Error())
		return
	}
	//
	bat := new(leveldb.Batch)
	for _, kv := range reqBody.KV {
		bat.Put([]byte(kv.K), []byte(kv.V))
	}
	err = DB.Write(bat, nil)
	if err != nil {
		_respJSONError(resp, lpi.StatusInternalServerError, err.Error())
		return
	}
	_respJSON(resp)
}

// leveldbGet 对leveldb执行get操作
func leveldbGet(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	go _logRequest(req)

	query := req.URL.Query()
	keyPrefix := query.Get("keyprefix")
	k := query.Get("key")
	//
	if keyPrefix == "" || k == "" {
		_respJSONError(resp, lpi.StatusBadRequest, "缺少必要的请求参数")
		return
	}
	//
	DB, err := getDBByKeyPrefix(keyPrefix)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, err.Error())
		return
	}
	//
	v, err := DB.Get([]byte(k), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			_respJSONError(resp, lpi.StatusNotFound, "未找到对应值")
		} else {
			logrus.Error(err)
			_respJSONError(resp, lpi.StatusInternalServerError, err.Error())
		}
		return
	}
	_respJSON(resp, string(v))
}

// leveldbRangeGet 对leveldb执行范围get操作
func leveldbRangeGet(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	go _logRequest(req)

	//
	query := req.URL.Query()
	keyPrefix := query.Get("keyprefix")
	start := query.Get("start")
	limit := query.Get("limit")
	if keyPrefix == "" || start == "" || limit == "" {
		_respJSONError(resp, lpi.StatusBadRequest, "缺少必要的请求参数")
		return
	}
	DB, err := getDBByKeyPrefix(keyPrefix)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, err.Error())
		return
	}
	kvs := make([]lpi.KVItemT, 0, 50)
	iter := DB.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(limit)}, nil)
	for iter.Next() {
		kvs = append(kvs, lpi.KVItemT{K: string(iter.Key()), V: string(iter.Value())})
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		logrus.Error(err)
		_respJSONError(resp, lpi.StatusInternalServerError, "系统异常")
		return
	}
	_respJSON(resp, kvs)
}

// leveldbGetByPrefix 根据前缀从leveldb进行批量查找
func leveldbGetByPrefix(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	go _logRequest(req)

	//
	query := req.URL.Query()
	keyPrefix := query.Get("keyprefix")
	key := query.Get("key")
	if keyPrefix == "" || key == "" {
		_respJSONError(resp, lpi.StatusBadRequest, "缺少必要的请求参数")
		return
	}
	DB, err := getDBByKeyPrefix(keyPrefix)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, err.Error())
		return
	}
	kvs := make([]lpi.KVItemT, 0, 100)
	iter := DB.NewIterator(util.BytesPrefix([]byte(key)), nil)
	for iter.Next() {
		kvs = append(kvs, lpi.KVItemT{K: string(iter.Key()), V: string(iter.Value())})
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		logrus.Error(err)
		_respJSONError(resp, lpi.StatusInternalServerError, "系统异常")
		return
	}
	_respJSON(resp, kvs)
}

// leveldbDel 对leveldb执行delete操作
func leveldbDel(resp http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// 支持batch模式
	/*
		请求体格式：
		{
			"keyprefix": "",
			"keys": []
		}
	*/
	go _logRequest(req)
	//
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, "请求体读取失败")
		return
	}
	//
	var reqBody lpi.DelReqBodyT
	if err = json.Unmarshal(body, &reqBody); err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, "请求体解析失败")
		return
	}
	//
	DB, err := getDBByKeyPrefix(reqBody.KeyPrefix)
	if err != nil {
		_respJSONError(resp, lpi.StatusBadRequest, err.Error())
		return
	}
	//
	bat := new(leveldb.Batch)
	for _, k := range reqBody.Keys {
		bat.Delete([]byte(k))
	}
	err = DB.Write(bat, nil)
	if err != nil {
		_respJSONError(resp, lpi.StatusInternalServerError, err.Error())
		return
	}
	_respJSON(resp)
}

func initDB() {
	dbList := appConf.Leveldbs
	dbs = make(map[string]*leveldb.DB, len(dbList))

	for dbName, keyPrefixs := range dbList {
		db, err := leveldb.OpenFile(dbName, nil)
		if err != nil {
			logrus.Fatalln(err)
			return
		}
		for _, kp := range keyPrefixs {
			dbs[kp] = db
		}
	}
}

func finishDB() {
	for _, db := range dbs {
		db.Close()
	}
}

func getDBByKeyPrefix(keyPrefix string) (ldb *leveldb.DB, err error) {
	var ok bool
	ldb, ok = dbs[keyPrefix]
	if !ok {
		err = fmt.Errorf("不存在对应键前缀 %s 的leveldb数据库", keyPrefix)
	}
	return
}

func initLogger() {
	customFormatter := logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.000000",
	}
	logrus.SetFormatter(&customFormatter)
	logrus.SetOutput(os.Stderr)
	logrus.SetLevel(logrus.DebugLevel)
}

func init() {
	flag.StringVar(&appConfPath, "c", "lpi.yaml", "db conf file path")
	flag.IntVar(&port, "p", 8991, "port to listen")
	flag.BoolVar(&debug, "d", false, "enbale debug mode")
	flag.Parse()

	initLogger()
}

// loadAppConf 加载配置文件
func loadAppConf(configFilePath string, t reflect.Type) (v interface{}) {
	confContent, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatalln(err)
	}
	v = reflect.New(t).Interface()
	if err = yaml.Unmarshal(confContent, v); err != nil {
		log.Fatalln(err)
	}
	return
}

func main() {
	appConf = *(loadAppConf(appConfPath, reflect.TypeOf(appConf)).(*appConfT))
	initDB()
	defer finishDB()

	//
	if debug {
		appConfBytes, _ := json.MarshalIndent(appConf, "", "  ")
		fmt.Println(string(appConfBytes))
	}

	router := httprouter.New()
	//
	router.POST("/leveldb/put", leveldbPut)
	router.GET("/leveldb/get", leveldbGet)
	router.GET("/leveldb/range-get", leveldbRangeGet)
	router.GET("/leveldb/prefix-get", leveldbGetByPrefix)
	router.POST("/leveldb/delete", leveldbDel)
	//
	logrus.Fatalln(http.ListenAndServe(fmt.Sprintf(":%d", port), router))
}
