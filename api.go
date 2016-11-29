package lpi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/Sirupsen/logrus"
)

var keyPrefix2APIBase map[string]string

// SetKeyPrefixAPIBaseM 设置键前缀到leveldb数据库的映射表
func SetKeyPrefixAPIBaseM(m map[string]string) {
	keyPrefix2APIBase = m
}

// httpGet 进行HTTP GET请求
func httpGet(url string, timeout time.Duration) (respBody []byte, err error) {
	hc := &http.Client{
		Timeout: timeout,
	}
	resp, err := hc.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("非正常响应，响应码为：%d", resp.StatusCode)
		return
	}

	respBody, err = ioutil.ReadAll(resp.Body)
	return
}

// httpPost 进行HTTP POST请求
func httpPost(url string, bodyType string, body io.Reader, timeout time.Duration) (respBody []byte, err error) {
	hc := &http.Client{
		Timeout: timeout,
	}
	resp, err := hc.Post(url, bodyType, body)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("非正常响应，响应码为：%d", resp.StatusCode)
		return
	}

	respBody, err = ioutil.ReadAll(resp.Body)
	return
}

func getJSON(url string, t reflect.Type) (v interface{}) {
	v = reflect.New(t).Interface()
	respBody, err := httpGet(url, 6*time.Second)
	if err != nil {
		logrus.Error(err, url)
		return
	}
	// 缓存API在没有查询到目标数据时，会返回空响应体
	if len(respBody) == 0 {
		return
	}
	if err = json.Unmarshal(respBody, v); err != nil {
		logrus.Error(err, url, string(respBody))
	}
	return
}

func getAPIByKeyPrefix(keyPrefix string) (apiBase string, err error) {
	apiBase, ok := keyPrefix2APIBase[keyPrefix]
	if !ok {
		err = fmt.Errorf("不存在对应键前缀 %s 的leveldb API", keyPrefix)
		logrus.Error(err)
	}
	return
}

// LevelDBT 指向一个leveldb服务
type LevelDBT struct {
	KeyPrefix string
}

// LeveldbPut 通过API向leveldb put数据
func (ldb LevelDBT) LeveldbPut(kvs map[string]string) (ok bool) {
	apiBase, err := getAPIByKeyPrefix(ldb.KeyPrefix)
	if err != nil {
		return false
	}
	url := fmt.Sprintf("%s/leveldb/put", apiBase)
	//
	reqBody := PutReqBodyT{
		KeyPrefix: ldb.KeyPrefix,
		KV:        make([]KVItemT, 0, len(kvs)),
	}
	for k, v := range kvs {
		reqBody.KV = append(reqBody.KV, KVItemT{K: k, V: v})
	}
	//
	reqBodyBytes, _ := json.Marshal(reqBody)
	respBody, err := httpPost(url, ContentTypeJSON, bytes.NewReader(reqBodyBytes), 10*time.Second)
	if err != nil {
		logrus.Error(err)
		ok = false
		return
	}
	var rBody RespBodyT
	if err = json.Unmarshal(respBody, &rBody); err != nil {
		logrus.Error(err)
		ok = false
		return
	}
	if rBody.Code != StatusOK {
		logrus.Error(rBody)
		ok = false
		return
	}
	ok = true
	return
}

// LeveldbGet 通过API向leveldb get数据
func (ldb LevelDBT) LeveldbGet(key string) (value string, ok bool) {
	apiBase, err := getAPIByKeyPrefix(ldb.KeyPrefix)
	if err != nil {
		return "", false
	}
	url := fmt.Sprintf("%s/leveldb/get/?keyprefix=%s&key=%s", apiBase, ldb.KeyPrefix, key)
	var rBody RespBodyT
	rBody = *(getJSON(url, reflect.TypeOf(rBody)).(*RespBodyT))
	if rBody.Code != StatusOK {
		if rBody.Code != StatusNotFound {
			logrus.Error(rBody)
		}
		ok = false
		return
	}
	value, ok = rBody.Data.(string), true
	return
}

// LeveldbRangeGet 通过API向leveldb get某个范围的数据
func (ldb LevelDBT) LeveldbRangeGet(start string, limit string) (kvs []KVItemT, ok bool) {
	apiBase, err := getAPIByKeyPrefix(ldb.KeyPrefix)
	if err != nil {
		return nil, false
	}
	url := fmt.Sprintf("%s/leveldb/range-get/?keyprefix=%s&start=%s&limit=%s", apiBase, ldb.KeyPrefix, start, limit)
	var rBody RespBodyT
	rBody = *(getJSON(url, reflect.TypeOf(rBody)).(*RespBodyT))
	if rBody.Code != StatusOK {
		if rBody.Code != StatusNotFound {
			logrus.Error(rBody)
		}
		ok = false
		return
	}
	iList := rBody.Data.([]interface{})
	kvs = make([]KVItemT, 0, len(iList))
	for _, i := range iList {
		im := i.(map[string]interface{})
		kvs = append(kvs, KVItemT{K: im["k"].(string), V: im["v"].(string)})
	}
	ok = true
	return
}

// LeveldbGetByPrefix 根据前缀来批量获取数据
func (ldb LevelDBT) LeveldbGetByPrefix(key string) (kvs []KVItemT, ok bool) {
	apiBase, err := getAPIByKeyPrefix(ldb.KeyPrefix)
	if err != nil {
		return nil, false
	}
	url := fmt.Sprintf("%s/leveldb/prefix-get/?keyprefix=%s&key=%s", apiBase, ldb.KeyPrefix, key)
	var rBody RespBodyT
	rBody = *(getJSON(url, reflect.TypeOf(rBody)).(*RespBodyT))
	if rBody.Code != StatusOK {
		if rBody.Code != StatusNotFound {
			logrus.Error(rBody)
		}
		ok = false
		return
	}
	iList := rBody.Data.([]interface{})
	kvs = make([]KVItemT, 0, len(iList))
	for _, i := range iList {
		im := i.(map[string]interface{})
		kvs = append(kvs, KVItemT{K: im["k"].(string), V: im["v"].(string)})
	}
	ok = true
	return
}

// LeveldbDel 通过API从leveldb中del数据
func (ldb LevelDBT) LeveldbDel(keyprefix string, keys []string) (ok bool) {
	apiBase, err := getAPIByKeyPrefix(ldb.KeyPrefix)
	if err != nil {
		return false
	}
	url := fmt.Sprintf("%s/leveldb/delete/", apiBase)
	//
	reqBody := DelReqBodyT{
		KeyPrefix: keyprefix,
		Keys:      keys,
	}
	reqBodyBytes, _ := json.Marshal(reqBody)
	respBody, err := httpPost(url, ContentTypeJSON, bytes.NewReader(reqBodyBytes), 6*time.Second)
	if err != nil {
		logrus.Error(err)
		ok = false
		return
	}
	var rBody RespBodyT
	if err = json.Unmarshal(respBody, &rBody); err != nil {
		logrus.Error(err)
		ok = false
		return
	}
	if rBody.Code != StatusOK {
		logrus.Error(rBody)
		ok = false
		return
	}
	ok = true
	return
}
