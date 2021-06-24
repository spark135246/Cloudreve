package task

import (
	"context"
	"encoding/json"
	model "github.com/cloudreve/Cloudreve/v3/models"
	"github.com/cloudreve/Cloudreve/v3/pkg/filesystem"
	"github.com/cloudreve/Cloudreve/v3/pkg/filesystem/driver/local"
	"github.com/cloudreve/Cloudreve/v3/pkg/filesystem/fsctx"
	"github.com/cloudreve/Cloudreve/v3/pkg/util"
	"path"
)

// ImportTask 导入务
type ImportTask struct {
	User      *model.User
	TaskModel *model.Task
	TaskProps ImportProps
	Err       *JobError
}

// ImportProps 导入任务属性
type ImportProps struct {
	PolicyID  uint   `json:"policy_id"`    // 存储策略ID
	Src       string `json:"src"`          // 原始路径
	Recursive bool   `json:"is_recursive"` // 是否递归导入
	Dst       string `json:"dst"`          // 目的目录
}

// Props 获取任务属性
func (job *ImportTask) Props() string {
	res, _ := json.Marshal(job.TaskProps)
	return string(res)
}

// Type 获取任务状态
func (job *ImportTask) Type() int {
	return ImportTaskType
}

// Creator 获取创建者ID
func (job *ImportTask) Creator() uint {
	return job.User.ID
}

// Model 获取任务的数据库模型
func (job *ImportTask) Model() *model.Task {
	return job.TaskModel
}

// SetStatus 设定状态
func (job *ImportTask) SetStatus(status int) {
	job.TaskModel.SetStatus(status)
}

// SetError 设定任务失败信息
func (job *ImportTask) SetError(err *JobError) {
	job.Err = err
	res, _ := json.Marshal(job.Err)
	job.TaskModel.SetError(string(res))
}

// SetErrorMsg 设定任务失败信息
func (job *ImportTask) SetErrorMsg(msg string, err error) {
	jobErr := &JobError{Msg: msg}
	if err != nil {
		jobErr.Error = err.Error()
	}
	job.SetError(jobErr)
}

// GetError 返回任务失败信息
func (job *ImportTask) GetError() *JobError {
	return job.Err
}

// Import 导入目录
func ImportDir(policyId uint, user *model.User, recursive bool, src string, dst string) error {
	ctx := context.Background()

	// 事务
	tx := model.DB.Begin()

	// 查找存储策略
	/*policy, err := model.GetPolicyByIDTransaction(policyId, tx)
	if err != nil {
		tx.Rollback()
		return err
	}*/

	// 创建文件系统
	//job.User.Policy = policy
	fs, err := filesystem.NewFileSystem(user)
	if err != nil {
		tx.Rollback()
		return err
	}
	fs.Tx = tx
	defer fs.Recycle()
	defer func() {
		fs.Tx = nil
	}()

	// 注册钩子
	fs.Use("BeforeAddFile", filesystem.HookValidateFile)
	fs.Use("BeforeAddFile", filesystem.HookValidateThumbnailExtension)
	fs.Use("BeforeAddFile", filesystem.HookValidateCapacityTransaction)
	fs.Use("AfterValidateFailed", filesystem.HookGiveBackCapacityTransaction)

	// 列取目录、对象
	coxIgnoreConflict := context.WithValue(context.Background(), fsctx.IgnoreDirectoryConflictCtx, true)
	objects, err := fs.Handler.List(ctx, src, recursive)
	if err != nil {
		tx.Rollback()
		return err
	}

	// 虚拟目录路径与folder对象ID的对应
	pathCache := make(map[string]*model.Folder, len(objects))

	// 插入目录记录到用户文件系统
	for _, object := range objects {
		if object.IsDir {
			// 创建目录
			virtualPath := path.Join(dst, object.RelativePath)
			folder, err := fs.CreateDirectoryTransaction(coxIgnoreConflict, virtualPath, tx)
			if err != nil {
				util.Log().Warning("导入任务无法创建用户目录[%s], %s", virtualPath, err)
			} else if folder.ID > 0 {
				pathCache[virtualPath] = folder
			}
		}
	}

	// 插入文件记录到用户文件系统
	for _, object := range objects {
		if !object.IsDir {
			// 创建文件信息
			virtualPath := path.Dir(path.Join(dst, object.RelativePath))
			fileHeader := local.FileStream{
				Size:        object.Size,
				VirtualPath: virtualPath,
				Name:        object.Name,
			}
			addFileCtx := context.WithValue(ctx, fsctx.FileHeaderCtx, fileHeader)
			addFileCtx = context.WithValue(addFileCtx, fsctx.SavePathCtx, object.Source)

			// 查找父目录
			parentFolder := &model.Folder{}
			if parent, ok := pathCache[virtualPath]; ok {
				parentFolder = parent
			} else {
				exist, folder := fs.IsPathExistTransaction(virtualPath, tx)
				if exist {
					parentFolder = folder
				} else {
					folder, err := fs.CreateDirectoryTransaction(context.Background(), virtualPath, tx)
					if err != nil {
						util.Log().Warning("导入任务无法创建用户目录[%s], %s",
							virtualPath, err)
						continue
					}
					parentFolder = folder
				}
			}

			// 插入文件记录
			file, err := fs.AddFileTransaction(addFileCtx, parentFolder, tx)
			if err != nil {
				util.Log().Warning("导入任务无法创插入文件[%s], %s",
					object.RelativePath, err)
				if err == filesystem.ErrInsufficientCapacity {
					tx.Rollback()
					return err
				}
			} else {
				// 异步生成缩略图
				fs.SetTargetFile(&[]model.File{*file})
			}

		}
	}

	// 提交事务
	if tx.Error != nil {
		tx.Rollback()
		util.Log().Error("导入文件提交前失败 %s", tx.Error)
		return err
	} else {
		tx.Commit()
	}

	// 生成缩略图
	if fs.User.Policy.IsThumbGenerateNeeded() {
		ctx := context.Background()
		fs.GenerateThumbnailsTransaction(ctx, nil)
	}
	return nil
}

// Do 开始执行任务
func (job *ImportTask) Do() {
	ctx := context.Background()

	// 事务
	tx := model.DB.Begin()

	// 查找存储策略
	policy, err := model.GetPolicyByIDTransaction(job.TaskProps.PolicyID, tx)
	if err != nil {
		job.SetErrorMsg("找不到存储策略", err)
		tx.Rollback()
		return
	}

	// 创建文件系统
	job.User.Policy = policy
	fs, err := filesystem.NewFileSystem(job.User)
	if err != nil {
		job.SetErrorMsg(err.Error(), nil)
		tx.Rollback()
		return
	}
	fs.Tx = tx
	defer fs.Recycle()
	defer func() {
		fs.Tx = nil
	}()

	// 注册钩子
	fs.Use("BeforeAddFile", filesystem.HookValidateFile)
	fs.Use("BeforeAddFile", filesystem.HookValidateThumbnailExtension)
	fs.Use("BeforeAddFile", filesystem.HookValidateCapacityTransaction)
	fs.Use("AfterValidateFailed", filesystem.HookGiveBackCapacityTransaction)

	// 列取目录、对象

	_ = job.TaskModel.SetProgressTransaction(ListingProgress, tx)
	coxIgnoreConflict := context.WithValue(context.Background(), fsctx.IgnoreDirectoryConflictCtx, true)
	objects, err := fs.Handler.List(ctx, job.TaskProps.Src, job.TaskProps.Recursive)
	if err != nil {
		job.SetErrorMsg("无法列取文件", err)
		tx.Rollback()
		return
	}

	_ = job.TaskModel.SetProgressTransaction(InsertingProgress, tx)

	// 虚拟目录路径与folder对象ID的对应
	pathCache := make(map[string]*model.Folder, len(objects))

	// 插入目录记录到用户文件系统
	for _, object := range objects {
		if object.IsDir {
			// 创建目录
			virtualPath := path.Join(job.TaskProps.Dst, object.RelativePath)
			folder, err := fs.CreateDirectoryTransaction(coxIgnoreConflict, virtualPath, tx)
			if err != nil {
				util.Log().Warning("导入任务无法创建用户目录[%s], %s", virtualPath, err)
			} else if folder.ID > 0 {
				pathCache[virtualPath] = folder
			}
		}
	}

	// 插入文件记录到用户文件系统
	for _, object := range objects {
		if !object.IsDir {
			// 创建文件信息
			virtualPath := path.Dir(path.Join(job.TaskProps.Dst, object.RelativePath))
			fileHeader := local.FileStream{
				Size:        object.Size,
				VirtualPath: virtualPath,
				Name:        object.Name,
			}
			addFileCtx := context.WithValue(ctx, fsctx.FileHeaderCtx, fileHeader)
			addFileCtx = context.WithValue(addFileCtx, fsctx.SavePathCtx, object.Source)

			// 查找父目录
			parentFolder := &model.Folder{}
			if parent, ok := pathCache[virtualPath]; ok {
				parentFolder = parent
			} else {
				exist, folder := fs.IsPathExistTransaction(virtualPath, tx)
				if exist {
					parentFolder = folder
				} else {
					folder, err := fs.CreateDirectoryTransaction(context.Background(), virtualPath, tx)
					if err != nil {
						util.Log().Warning("导入任务无法创建用户目录[%s], %s",
							virtualPath, err)
						continue
					}
					parentFolder = folder
				}
			}

			// 插入文件记录
			file, err := fs.AddFileTransaction(addFileCtx, parentFolder, tx)
			if err != nil {
				util.Log().Warning("导入任务无法创插入文件[%s], %s",
					object.RelativePath, err)
				if err == filesystem.ErrInsufficientCapacity {
					job.SetErrorMsg("容量不足", err)
					tx.Rollback()
					return
				}
			} else {
				// 异步生成缩略图
				fs.SetTargetFile(&[]model.File{*file})
			}

		}
	}

	// 提交事务
	if tx.Error != nil {
		tx.Rollback()
		job.SetErrorMsg("导入文件错误", tx.Error)
		util.Log().Error("导入文件提交前失败 %s", tx.Error)
	} else {
		tx.Commit()
	}

	// 生成缩略图
	if fs.User.Policy.IsThumbGenerateNeeded() {
		ctx := context.Background()
		fs.GenerateThumbnailsTransaction(ctx, nil)
	}

}

// NewImportTask 新建导入任务
func NewImportTask(user, policy uint, src, dst string, recursive bool) (Job, error) {
	creator, err := model.GetActiveUserByID(user)
	if err != nil {
		return nil, err
	}

	newTask := &ImportTask{
		User: &creator,
		TaskProps: ImportProps{
			PolicyID:  policy,
			Recursive: recursive,
			Src:       src,
			Dst:       dst,
		},
	}

	record, err := Record(newTask)
	if err != nil {
		return nil, err
	}
	newTask.TaskModel = record

	return newTask, nil
}

// NewImportTaskFromModel 从数据库记录中恢复导入任务
func NewImportTaskFromModel(task *model.Task) (Job, error) {
	user, err := model.GetActiveUserByID(task.UserID)
	if err != nil {
		return nil, err
	}
	newTask := &ImportTask{
		User:      &user,
		TaskModel: task,
	}

	err = json.Unmarshal([]byte(task.Props), &newTask.TaskProps)
	if err != nil {
		return nil, err
	}

	return newTask, nil
}
