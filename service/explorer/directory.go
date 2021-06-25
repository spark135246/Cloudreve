package explorer

import (
	"context"
	"fmt"
	model "github.com/cloudreve/Cloudreve/v3/models"
	"github.com/cloudreve/Cloudreve/v3/pkg/task"
	"github.com/cloudreve/Cloudreve/v3/pkg/util"
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
	var src string
	rex, _ := regexp.Compile(`^(\/root)$|^(\/root\/)`)
	if rex.MatchString(service.Path) {
		// 拆分
		if service.Path == "/root" {
			src = "/data"
		} else {
			src = "/data/" + service.Path[5:]
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
	rex, _ = regexp.Compile(`^(\/root)$|^(\/root\/)`)
	if rex.MatchString(service.Path) {
		//
		responseObjects, err := fs.Handler.List(ctx, src, false)
		if err != nil {
			util.Log().Error("取得列表错误 %s", err.Error())
			return serializer.Err(serializer.CodeNotSet, err.Error(), err)
		}
		files, folders, err := fs.List1(ctx, service.Path, nil)
		if err != nil {
			util.Log().Error("取得列表错误 %s", err.Error())
			return serializer.Err(serializer.CodeNotSet, err.Error(), err)
		}
		// 获取要删除的文件
		deleteFiles := make([]model.File, 0)
		deleteFolder := make([]model.Folder, 0)
		for _, file := range files {
			ok := true
			for _, object := range responseObjects {
				if object.Source == file.SourceName {
					ok = false
					break
				}
			}
			if ok {
				deleteFiles = append(deleteFiles, file)
			}
		}
		for _, folder := range folders {
			ok := true
			for _, object := range objects {
				if object.Name == folder.Name {
					ok = false
					break
				}
			}
			if ok {
				deleteFolder = append(deleteFolder, folder)
			}

		}
		// 删除文件
		deleteFileIds := make([]uint, len(deleteFiles))
		deleteFolderIds := make([]uint, len(deleteFolder))
		for _, v := range deleteFiles {
			deleteFileIds = append(deleteFileIds, v.ID)
		}
		for _, v := range deleteFolder {
			deleteFolderIds = append(deleteFolderIds, v.ID)
		}
		fs.DirTarget = nil
		if len(deleteFileIds) > 0 || len(deleteFolderIds) > 0 {
			err = fs.DeleteTransaction(ctx, deleteFolderIds, deleteFileIds, true, nil)
			if err != nil {
				util.Log().Error("删除错误 ", err.Error())
				return serializer.Err(serializer.CodeNotSet, err.Error(), err)
			}
		}
		// 获取子项目
		objects, err = fs.List(ctx, service.Path, nil)
		if err != nil {
			return serializer.Err(serializer.CodeNotSet, err.Error(), err)
		}
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
