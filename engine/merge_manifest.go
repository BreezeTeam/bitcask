package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	mergeManifestName    = "merge.manifest"
	mergeManifestTemp    = "merge.manifest.tmp"
	mergeManifestMagic   = uint32(0x4d52474d)
	mergeManifestVersion = uint16(1)
	mergeManifestSize    = 38
)

type mergeManifestPhase uint16

const (
	mergeManifestPrepared mergeManifestPhase = iota + 1
	mergeManifestInstalled
)

type mergeManifest struct {
	Phase             mergeManifestPhase
	SourceFileID      int64
	FirstTargetFileID int64
	LastTargetFileID  int64
}

func (db *DB) writeMergeManifest(manifest mergeManifest) error {
	buf := make([]byte, mergeManifestSize)
	binary.LittleEndian.PutUint32(buf[0:4], mergeManifestMagic)
	binary.LittleEndian.PutUint16(buf[4:6], mergeManifestVersion)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(manifest.Phase))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(manifest.SourceFileID))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(manifest.FirstTargetFileID))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(manifest.LastTargetFileID))
	binary.LittleEndian.PutUint32(buf[32:36], crc32.ChecksumIEEE(buf[:32]))
	binary.LittleEndian.PutUint16(buf[36:38], mergeManifestSize)
	return writeMetadataAtomically(
		db.opt,
		filepath.Join(db.opt.Dir, mergeManifestTemp),
		filepath.Join(db.opt.Dir, mergeManifestName),
		buf,
		0644,
	)
}

func readMergeManifest(dir string) (mergeManifest, error) {
	buf, err := os.ReadFile(filepath.Join(dir, mergeManifestName))
	if err != nil {
		return mergeManifest{}, err
	}
	if len(buf) != mergeManifestSize || binary.LittleEndian.Uint32(buf[0:4]) != mergeManifestMagic || binary.LittleEndian.Uint16(buf[4:6]) != mergeManifestVersion || binary.LittleEndian.Uint16(buf[36:38]) != mergeManifestSize {
		return mergeManifest{}, errors.New("invalid merge manifest")
	}
	if crc32.ChecksumIEEE(buf[:32]) != binary.LittleEndian.Uint32(buf[32:36]) {
		return mergeManifest{}, errors.New("corrupt merge manifest")
	}
	return mergeManifest{
		Phase:             mergeManifestPhase(binary.LittleEndian.Uint16(buf[6:8])),
		SourceFileID:      int64(binary.LittleEndian.Uint64(buf[8:16])),
		FirstTargetFileID: int64(binary.LittleEndian.Uint64(buf[16:24])),
		LastTargetFileID:  int64(binary.LittleEndian.Uint64(buf[24:32])),
	}, nil
}

func recoverMergeManifest(opt Options) error {
	path := filepath.Join(opt.Dir, mergeManifestName)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	manifest, err := readMergeManifest(opt.Dir)
	if err != nil {
		return err
	}
	if manifest.Phase != mergeManifestPrepared && manifest.Phase != mergeManifestInstalled {
		return errors.New("unknown merge manifest phase")
	}
	targetsExist := true
	for id := manifest.FirstTargetFileID; id <= manifest.LastTargetFileID; id++ {
		if _, err := os.Stat(getDataFilePath(opt.Dir, id)); err != nil {
			targetsExist = false
			break
		}
	}
	if manifest.Phase == mergeManifestInstalled && targetsExist {
		if err := os.Remove(getDataFilePath(opt.Dir, manifest.SourceFileID)); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := syncDirectory(opt.Dir); err != nil {
			return err
		}
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return runMetadataOperation(opt, metadataStageManifestDeleteDirSync, func() error {
		return syncDirectory(opt.Dir)
	})
}

func (db *DB) clearMergeManifest() error {
	if err := os.Remove(filepath.Join(db.opt.Dir, mergeManifestName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return runMetadataOperation(db.opt, metadataStageManifestDeleteDirSync, func() error {
		return syncDirectory(db.opt.Dir)
	})
}
