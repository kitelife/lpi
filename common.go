package lpi

// KVItemT 键值组合
type KVItemT struct {
	K string `json:"k"`
	V string `json:"v"`
}

// PutReqBodyT leveldb put请求体的结构定义
type PutReqBodyT struct {
	KeyPrefix string `json:"keyprefix"`
	KV        []KVItemT
}

// DelReqBodyT leveldb delete请求体的结构定义
type DelReqBodyT struct {
	KeyPrefix string   `json:"keyprefix"`
	Keys      []string `json:"keys"`
}

// RespBodyT leveldb api响应体结构定义
type RespBodyT struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

// -----------------------------------------------------------------------------

const (
	// StatusOK 正确响应
	StatusOK = 200
	// StatusBadRequest 错误请求
	StatusBadRequest = 400
	// StatusForbidden 禁止访问
	StatusForbidden = 403
	// StatusInternalServerError 内部错误
	StatusInternalServerError = 500
	// StatusNotFound 未找到指定资源
	StatusNotFound = 404
	// StatusUnknownFailed 不明原因的失败
	StatusUnknownFailed = 520
)

// StatusMsgs 响应状态信息
var StatusMsgs = map[int]string{
	StatusOK:                  "成功",
	StatusBadRequest:          "非法的请求",
	StatusForbidden:           "禁止访问",
	StatusInternalServerError: "服务器内部错误",
	StatusNotFound:            "未找到指定资源",
}

// ContentTypeJSON JSON格式请求/响应体
const ContentTypeJSON = "application/json"
