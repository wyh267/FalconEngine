package index

import "github.com/FalconEngine/tools"

type FalconIndexService interface {

	// 新建mapping
	CreateMappings(mappings *tools.FalconIndexMappings) error

	// 添加文档
	UpdateDocument(documentID string,document map[string]interface{}) error

	// 删除文档
	DeleteDocument(documentID string) error
	// 查询


}