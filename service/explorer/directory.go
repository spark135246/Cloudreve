package explorer

import (
	"context"
	"fmt"
	model "github.com/cloudreve/Cloudreve/v3/models"
	"github.com/cloudreve/Cloudreve/v3/pkg/task"
	"regexp"

	"github.com/cloudreve/Cloudreve/v3/pkg/filesystem"
	"github.com/cloudreve/Cloudreve/v3/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v3/pkg/serializer"
	"github.com/gin-gonic/gin"
)

// DirectoryService 创建新目录服务
type DirectoryService struct {
	Path string `uri:"path" json:"path" binding:"required,min=1,max=65535"`
}

// ListDirectory 列出目录内容
func (service *DirectoryService) ListDirectory(c *gin.Context) serializer.Response {
	// 创建文件系统
	fs, err := filesystem.NewFileSystemFromContext(c)
	if err != nil {
		return serializer.Err(serializer.CodePolicyNotAllowed, err.Error(), err)
	}
	defer fs.Recycle()

	// 判断根目录，导入根目录文件
	rex, _ := regexp.Compile(`^(\/root)$|^(\/root\/)`)
	if rex.MatchString(service.Path) {
		var src string
		// 拆分
		if service.Path == "/root" {
			src = "root"
		} else {
			src = "root/" + service.Path[5:]
		}
		// 导入目录
		fmt.Println("开始导入目录")
		if user, _ := c.Get("user"); user != nil {
			if u, ok := user.(*model.User); ok {
				err = task.ImportDir(u.Policy.ID, u, false, src, "/"+src)
				if err != nil {
					fmt.Println("导入错误")
				}
			}
		}
	}

	// 上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 获取子项目
	objects, err := fs.List(ctx, service.Path, nil)
	if err != nil {
		return serializer.Err(serializer.CodeNotSet, err.Error(), err)
	}

	var parentID uint
	if len(fs.DirTarget) > 0 {
		parentID = fs.DirTarget[0].ID
	}

	return serializer.Response{
		Code: 0,
		Data: map[string]interface{}{
			"parent":  hashid.HashID(parentID, hashid.FolderID),
			"objects": objects,
		},
	}
}

// CreateDirectory 创建目录
func (service *DirectoryService) CreateDirectory(c *gin.Context) serializer.Response {
	// 创建文件系统
	fs, err := filesystem.NewFileSystemFromContext(c)
	if err != nil {
		return serializer.Err(serializer.CodePolicyNotAllowed, err.Error(), err)
	}
	defer fs.Recycle()

	// 上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建目录
	_, err = fs.CreateDirectory(ctx, service.Path)
	if err != nil {
		return serializer.Err(serializer.CodeCreateFolderFailed, err.Error(), err)
	}
	return serializer.Response{
		Code: 0,
	}

}
