package bitcask

import (
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

type metadataStage string

const (
	metadataStageTempWrite             metadataStage = "temp-write"
	metadataStageFileSync              metadataStage = "file-sync"
	metadataStageRename                metadataStage = "rename"
	metadataStageDirSync               metadataStage = "directory-sync"
	metadataStageManifestDeleteDirSync metadataStage = "manifest-delete-directory-sync"
)

func writeMetadataAtomically(opt Options, tempPath, finalPath string, data []byte, perm os.FileMode) error {
	var file *os.File
	if err := runMetadataOperation(opt, metadataStageTempWrite, func() error {
		var err error
		file, err = os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
		if err != nil {
			return err
		}
		n, err := file.Write(data)
		if err == nil && n != len(data) {
			return io.ErrShortWrite
		}
		return err
	}); err != nil {
		if file != nil {
			_ = file.Close()
		}
		return err
	}
	if err := runMetadataOperation(opt, metadataStageFileSync, file.Sync); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := runMetadataOperation(opt, metadataStageRename, func() error {
		return os.Rename(tempPath, finalPath)
	}); err != nil {
		return err
	}
	return runMetadataOperation(opt, metadataStageDirSync, func() error {
		return syncDirectory(filepath.Dir(finalPath))
	})
}

func runMetadataOperation(opt Options, stage metadataStage, operation func() error) error {
	fault := opt.FaultInjection
	if fault.Enable && fault.MetadataStage == string(stage) && opt.faultState != nil {
		occurrence := atomic.AddInt64(&opt.faultState.metadata, 1)
		if failAfter(fault.MetadataFailAfter, occurrence) {
			return ErrFaultInjectedMetadata
		}
	}
	return operation()
}
