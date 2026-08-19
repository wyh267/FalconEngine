package transport

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// CodecName JSON 消息编码名（content-subtype）
const CodecName = "json"

// jsonCodec gRPC 自定义 JSON 编解码器：
// 用 JSON 代替 protobuf 做消息序列化，避免对 protoc 工具链的依赖。
type jsonCodec struct{}

func (jsonCodec) Name() string { return CodecName }

func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func init() {
	encoding.RegisterCodec(jsonCodec{})
}
