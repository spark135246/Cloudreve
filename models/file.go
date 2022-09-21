package model

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"path"
	"time"

	"github.com/cloudreve/Cloudreve/v3/pkg/util"
	"github.com/jinzhu/gorm"
)

// File 文件
type File struct {
	// 表字段
	gorm.Model
	Name            string `gorm:"unique_index:idx_only_one"`
	SourceName      string `gorm:"type:text"`
	UserID          uint   `gorm:"index:user_id;unique_index:idx_only_one"`
	Size            uint64
	PicInfo         string
	FolderID        uint `gorm:"index:folder_id;unique_index:idx_only_one"`
	PolicyID        uint
	UploadSessionID *string `gorm:"index:session_id;unique_index:session_only_one"`
	Metadata        string  `gorm:"type:text"`

	// 关联模型
	Policy Policy `gorm:"PRELOAD:false,association_autoupdate:false"`

	// 数据库忽略字段
	Position           string            `gorm:"-"`
	MetadataSerialized map[string]string `gorm:"-"`
}

func init() {
	// 注册缓存用到的复杂结构
	gob.Register(File{})
}

// Create 创建文件记录
func (file *File) Create() error {
	tx := DB.Begin()

	if err := tx.Create(file).Error; err != nil {
		util.Log().Warning("无法插入文件记录, %s", err)
		tx.Rollback()
		return err
	}

	user := &User{}
	user.ID = file.UserID
	if err := user.ChangeStorage(tx, "+", file.Size); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

// AfterFind 找到文件后的钩子
func (file *File) AfterFind() (err error) {
	// 反序列化文件元数据
	if file.Metadata != "" {
		err = json.Unmarshal([]byte(file.Metadata), &file.MetadataSerialized)
	}

	return
}

// BeforeSave Save策略前的钩子
func (file *File) BeforeSave() (err error) {
	metaValue, err := json.Marshal(&file.MetadataSerialized)
	file.Metadata = string(metaValue)
	return err
}

// Create 创建文件记录
func (file *File) CreateTransaction(tx *gorm.DB) (uint, error) {
	if err := tx.Create(file).Error; err != nil {
		util.Log().Warning("无法插入文件记录, %s", err)
		return 0, err
	}
	return file.ID, nil
}

// GetChildFile 查找目录下名为name的子文件
func (folder *Folder) GetChildFile(name string) (*File, error) {
	var file File
	result := DB.Where("folder_id = ? AND name = ?", folder.ID, name).Find(&file)

	if result.Error == nil {
		file.Position = path.Join(folder.Position, folder.Name)
	}
	return &file, result.Error
}

// GetChildFile 查找目录下名为name的子文件
func (folder *Folder) GetChildFileTransaction(name string, tx *gorm.DB) (*File, error) {
	var file File
	result := tx.Where("folder_id = ? AND name = ?", folder.ID, name).Find(&file)

	if result.Error == nil {
		file.Position = path.Join(folder.Position, folder.Name)
	}
	return &file, result.Error
}

// GetChildFiles 查找目录下子文件
func (folder *Folder) GetChildFiles() ([]File, error) {
	var files []File
	result := DB.Where("folder_id = ?", folder.ID).Find(&files)

	if result.Error == nil {
		for i := 0; i < len(files); i++ {
			files[i].Position = path.Join(folder.Position, folder.Name)
		}
	}
	return files, result.Error
}

// GetChildFilesTransaction 查找目录下子文件
func (folder *Folder) GetChildFilesTransaction(tx *gorm.DB) ([]File, error) {
	var files []File
	result := tx.Where("folder_id = ?", folder.ID).Find(&files)

	if result.Error == nil {
		for i := 0; i < len(files); i++ {
			files[i].Position = path.Join(folder.Position, folder.Name)
		}
	}
	return files, result.Error
}

// GetFilesByIDs 根据文件ID批量获取文件,
// UID为0表示忽略用户，只根据文件ID检索
func GetFilesByIDs(ids []uint, uid uint) ([]File, error) {
	return GetFilesByIDsFromTX(DB, ids, uid)
}

func GetFilesByIDsFromTX(tx *gorm.DB, ids []uint, uid uint) ([]File, error) {
	var files []File
	var result *gorm.DB
	if uid == 0 {
		result = tx.Where("id in (?)", ids).Find(&files)
	} else {
		result = tx.Where("id in (?) AND user_id = ?", ids, uid).Find(&files)
	}
	return files, result.Error
}

// GetFilesByIDs 根据文件ID批量获取文件,
// UID为0表示忽略用户，只根据文件ID检索
func GetFilesByIDsTransaction(ids []uint, uid uint, tx *gorm.DB) ([]File, error) {
	var files []File

	// 循环寻找
	if uid == 0 {
		for _, id := range ids {
			var file File
			err := tx.Where("id = ?", id).First(&file).Error
			// 找不到记录
			if gorm.IsRecordNotFoundError(err) {
				continue
			} else if err != nil { // 其他错误
				util.Log().Error("根据文件ID批量获取文件 %s", err.Error())
				return nil, err
			}
			files = append(files, file)
		}
	} else {
		for _, id := range ids {
			var file File
			err := tx.Where("id = ? AND user_id = ?", id, uid).First(&file).Error
			// 找不到记录
			if gorm.IsRecordNotFoundError(err) {
				continue
			} else if err != nil { // 其他错误
				util.Log().Error("根据文件ID批量获取文件 %s", err.Error())
				return nil, err
			}
			files = append(files, file)
		}
	}
	return files, nil
}

// GetFilesByKeywords 根据关键字搜索文件,
// UID为0表示忽略用户，只根据文件ID检索. 如果 parents 非空， 则只限制在 parent 包含的目录下搜索
func GetFilesByKeywords(uid uint, parents []uint, keywords ...interface{}) ([]File, error) {
	var (
		files      []File
		result     = DB
		conditions string
	)

	// 生成查询条件
	for i := 0; i < len(keywords); i++ {
		conditions += "name like ?"
		if i != len(keywords)-1 {
			conditions += " or "
		}
	}

	if uid != 0 {
		result = result.Where("user_id = ?", uid)
	}

	if len(parents) > 0 {
		result = result.Where("folder_id in (?)", parents)
	}

	result = result.Where("("+conditions+")", keywords...).Find(&files)

	return files, result.Error
}

// GetChildFilesOfFolders 批量检索目录子文件
func GetChildFilesOfFolders(folders *[]Folder) ([]File, error) {
	// 将所有待检索目录ID抽离，以便检索文件
	folderIDs := make([]uint, 0, len(*folders))
	for _, value := range *folders {
		folderIDs = append(folderIDs, value.ID)
	}

	// 检索文件
	var files []File
	result := DB.Where("folder_id in (?)", folderIDs).Find(&files)
	return files, result.Error
}

// GetChildFilesOfFolders 批量检索目录子文件
func GetChildFilesOfFoldersTransaction(folders *[]Folder, tx *gorm.DB) ([]File, error) {
	// 将所有待删除目录ID抽离，以便检索文件
	folderIDs := make([]uint, 0, len(*folders))
	for _, value := range *folders {
		folderIDs = append(folderIDs, value.ID)
	}

	// 检索文件
	var files []File
	for _, folderID := range folderIDs {
		var filesResult []File
		err := tx.Where("folder_id = ?", folderID).Find(&filesResult).Error
		// 找不到记录
		if gorm.IsRecordNotFoundError(err) {
			continue
		} else if err != nil {
			util.Log().Error("批量检索目录子文件错误 %s", err.Error())
			return nil, err
		}
		files = append(files, filesResult...)
	}
	return files, nil
}

// GetUploadPlaceholderFiles 获取所有上传占位文件
// UID为0表示忽略用户
func GetUploadPlaceholderFiles(uid uint) []*File {
	query := DB
	if uid != 0 {
		query = query.Where("user_id = ?", uid)
	}

	var files []*File
	query.Where("upload_session_id is not NULL").Find(&files)
	return files
}

// GetPolicy 获取文件所属策略
func (file *File) GetPolicy() *Policy {
	if file.Policy.Model.ID == 0 {
		file.Policy, _ = GetPolicyByID(file.PolicyID)
	}
	return &file.Policy
}

// RemoveFilesWithSoftLinks 去除给定的文件列表中有软链接的文件
func RemoveFilesWithSoftLinks(files []File) ([]File, error) {
	// 结果值
	filteredFiles := make([]File, 0)

	// 查询软链接的文件
	var filesWithSoftLinks []File
	tx := DB
	for key, value := range files {
		// 防止参数过多消耗内存,默认999
		if key > 0 && key%333 == 0 {
			var filesWithSoftLinks1 []File
			result := tx.Find(&filesWithSoftLinks1)
			if result.Error != nil {
				return nil, result.Error
			}
			filesWithSoftLinks = append(filesWithSoftLinks, filesWithSoftLinks1...)
			tx = DB
		}
		tx = tx.Or("source_name = ? and policy_id = ? and id != ?", value.SourceName, value.PolicyID, value.ID)
	}

	// 过滤具有软连接的文件
	// TODO: 优化复杂度
	if len(filesWithSoftLinks) == 0 {
		filteredFiles = files
	} else {
		for i := 0; i < len(files); i++ {
			finder := false
			for _, value := range filesWithSoftLinks {
				if value.PolicyID == files[i].PolicyID && value.SourceName == files[i].SourceName {
					finder = true
					break
				}
			}
			if !finder {
				filteredFiles = append(filteredFiles, files[i])
			}

		}
	}

	return filteredFiles, nil

}

<<<<<<< HEAD
// RemoveFilesWithSoftLinks 去除给定的文件列表中有软链接的文件
func RemoveFilesWithSoftLinksTransaction(files []File, tx *gorm.DB) ([]File, error) {
	// 结果值
	filteredFiles := make([]File, 0)

	for _, value := range files {
		var filesResult []File
		result := tx.Where("source_name = ? and policy_id = ? and id != ?", value.SourceName, value.PolicyID, value.ID).Find(&filesResult)
		if result.Error != nil {
			return nil, result.Error
		}

		// 找不到
		if len(filesResult) == 0 {
			filteredFiles = append(filteredFiles, value)
		}
	}

	return filteredFiles, nil

}

// DeleteFileByIDs 根据给定ID批量删除文件记录
func DeleteFileByIDs(ids []uint) error {
	// 超出数量切分
	count := len(ids) / 999
	for i := 0; i <= count; i++ {
		var result *gorm.DB
		if i == count {
			result = DB.Where("id in (?)", ids[i*999:]).Unscoped().Delete(&File{})
		} else {
			result = DB.Where("id in (?)", ids[i*999:(i+1)*999]).Unscoped().Delete(&File{})
		}
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// DeleteFileByIDs 根据给定ID批量删除文件记录
func DeleteFileByIDsTransaction(ids []uint, tx *gorm.DB) error {
	for _, id := range ids {
		result := tx.Where("id = ?", id).Unscoped().Delete(&File{})
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
=======
// DeleteFiles 批量删除文件记录并归还容量
func DeleteFiles(files []*File, uid uint) error {
	tx := DB.Begin()
	user := &User{}
	user.ID = uid
	var size uint64
	for _, file := range files {
		if file.UserID != uid {
			tx.Rollback()
			return errors.New("user id not consistent")
		}

		result := tx.Unscoped().Where("size = ?", file.Size).Delete(file)
		if result.Error != nil {
			tx.Rollback()
			return result.Error
		}

		if result.RowsAffected == 0 {
			tx.Rollback()
			return errors.New("file size is dirty")
		}

		size += file.Size
	}

	if err := user.ChangeStorage(tx, "-", size); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
>>>>>>> upstream/master
}

// GetFilesByParentIDs 根据父目录ID查找文件
func GetFilesByParentIDs(ids []uint, uid uint) ([]File, error) {
	files := make([]File, 0, len(ids))
	result := DB.Where("user_id = ? and folder_id in (?)", uid, ids).Find(&files)
	return files, result.Error
}

// GetFilesByUploadSession 查找上传会话对应的文件
func GetFilesByUploadSession(sessionID string, uid uint) (*File, error) {
	file := File{}
	result := DB.Where("user_id = ? and upload_session_id = ?", uid, sessionID).Find(&file)
	return &file, result.Error
}

// Rename 重命名文件
func (file *File) Rename(new string) error {
	return DB.Model(&file).UpdateColumn("name", new).Error
}

// UpdatePicInfo 更新文件的图像信息
func (file *File) UpdatePicInfo(value string) error {
	return DB.Model(&file).Set("gorm:association_autoupdate", false).UpdateColumns(File{PicInfo: value}).Error
}

// UpdatePicInfo 更新文件的图像信息
func (file *File) UpdatePicInfoTransaction(value string, tx *gorm.DB) error {
	return tx.Model(&file).Update("pic_info", value).Error
}

// UpdateSize 更新文件的大小信息
// TODO: 全局锁
func (file *File) UpdateSize(value uint64) error {
	tx := DB.Begin()
	var sizeDelta uint64
	operator := "+"
	user := User{}
	user.ID = file.UserID
	if value > file.Size {
		sizeDelta = value - file.Size
	} else {
		operator = "-"
		sizeDelta = file.Size - value
	}

	if res := tx.Model(&file).
		Where("size = ?", file.Size).
		Set("gorm:association_autoupdate", false).
		Update("size", value); res.Error != nil {
		tx.Rollback()
		return res.Error
	}

	if err := user.ChangeStorage(tx, operator, sizeDelta); err != nil {
		tx.Rollback()
		return err
	}

	file.Size = value
	return tx.Commit().Error
}

// UpdateSourceName 更新文件的源文件名
func (file *File) UpdateSourceName(value string) error {
	return DB.Model(&file).Set("gorm:association_autoupdate", false).Update("source_name", value).Error
}

func (file *File) PopChunkToFile(lastModified *time.Time, picInfo string) error {
	file.UploadSessionID = nil
	if lastModified != nil {
		file.UpdatedAt = *lastModified
	}

	return DB.Model(file).UpdateColumns(map[string]interface{}{
		"upload_session_id": file.UploadSessionID,
		"updated_at":        file.UpdatedAt,
		"pic_info":          picInfo,
	}).Error
}

// CanCopy 返回文件是否可被复制
func (file *File) CanCopy() bool {
	return file.UploadSessionID == nil
}

/*
	实现 webdav.FileInfo 接口
*/

func (file *File) GetName() string {
	return file.Name
}

func (file *File) GetSize() uint64 {
	return file.Size
}
func (file *File) ModTime() time.Time {
	return file.UpdatedAt
}

func (file *File) IsDir() bool {
	return false
}

func (file *File) GetPosition() string {
	return file.Position
}
