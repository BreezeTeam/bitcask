package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	valueLogGCManifestName      = "value-gc.manifest"
	valueLogGCManifestTemp      = "value-gc.manifest.tmp"
	valueLogGCManifestMagic     = uint32(0x5647434d)
	valueLogGCManifestVersion   = uint16(2)
	valueLogGCManifestSize      = 38
	valueLogGCManifestV1Version = uint16(1)
	valueLogGCManifestV1Size    = 22
)

type valueLogGCPhase uint16

const (
	valueLogGCPrepared valueLogGCPhase = iota + 1
	valueLogGCPointersInstalled
	valueLogGCSourceRemoved
	valueLogGCFinalized
)

type valueLogGCManifest struct {
	Phase                  valueLogGCPhase
	SourceFileID           uint64
	FirstReplacementFileID uint64
	LastReplacementFileID  uint64
	legacy                 bool
}

func (db *DB) writeValueLogGCManifest(manifest valueLogGCManifest) error {
	buf := make([]byte, valueLogGCManifestSize)
	binary.LittleEndian.PutUint32(buf[0:4], valueLogGCManifestMagic)
	binary.LittleEndian.PutUint16(buf[4:6], valueLogGCManifestVersion)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(manifest.Phase))
	binary.LittleEndian.PutUint64(buf[8:16], manifest.SourceFileID)
	binary.LittleEndian.PutUint64(buf[16:24], manifest.FirstReplacementFileID)
	binary.LittleEndian.PutUint64(buf[24:32], manifest.LastReplacementFileID)
	binary.LittleEndian.PutUint32(buf[32:36], crc32.ChecksumIEEE(buf[:32]))
	binary.LittleEndian.PutUint16(buf[36:38], uint16(valueLogGCManifestSize))
	return writeMetadataAtomically(
		db.opt,
		filepath.Join(db.opt.Dir, valueLogGCManifestTemp),
		filepath.Join(db.opt.Dir, valueLogGCManifestName),
		buf,
		0644,
	)
}

func readValueLogGCManifest(dir string) (valueLogGCManifest, error) {
	buf, err := os.ReadFile(filepath.Join(dir, valueLogGCManifestName))
	if err != nil {
		return valueLogGCManifest{}, err
	}
	if len(buf) < 8 || binary.LittleEndian.Uint32(buf[0:4]) != valueLogGCManifestMagic {
		return valueLogGCManifest{}, errors.New("invalid value-log GC manifest")
	}
	switch binary.LittleEndian.Uint16(buf[4:6]) {
	case valueLogGCManifestV1Version:
		if len(buf) != valueLogGCManifestV1Size || binary.LittleEndian.Uint16(buf[20:22]) != valueLogGCManifestV1Size {
			return valueLogGCManifest{}, errors.New("invalid value-log GC manifest")
		}
		if crc32.ChecksumIEEE(buf[:16]) != binary.LittleEndian.Uint32(buf[16:20]) {
			return valueLogGCManifest{}, errors.New("corrupt value-log GC manifest")
		}
		manifest := valueLogGCManifest{
			Phase:        valueLogGCPhase(binary.LittleEndian.Uint16(buf[6:8])),
			SourceFileID: binary.LittleEndian.Uint64(buf[8:16]),
			legacy:       true,
		}
		if manifest.Phase != valueLogGCPrepared && manifest.Phase != valueLogGCPointersInstalled {
			return valueLogGCManifest{}, errors.New("unknown value-log GC manifest phase")
		}
		return manifest, nil
	case valueLogGCManifestVersion:
		if len(buf) != valueLogGCManifestSize || binary.LittleEndian.Uint16(buf[36:38]) != valueLogGCManifestSize {
			return valueLogGCManifest{}, errors.New("invalid value-log GC manifest")
		}
		if crc32.ChecksumIEEE(buf[:32]) != binary.LittleEndian.Uint32(buf[32:36]) {
			return valueLogGCManifest{}, errors.New("corrupt value-log GC manifest")
		}
		manifest := valueLogGCManifest{
			Phase:                  valueLogGCPhase(binary.LittleEndian.Uint16(buf[6:8])),
			SourceFileID:           binary.LittleEndian.Uint64(buf[8:16]),
			FirstReplacementFileID: binary.LittleEndian.Uint64(buf[16:24]),
			LastReplacementFileID:  binary.LittleEndian.Uint64(buf[24:32]),
		}
		if manifest.Phase < valueLogGCPrepared || manifest.Phase > valueLogGCFinalized || manifest.FirstReplacementFileID > manifest.LastReplacementFileID {
			return valueLogGCManifest{}, errors.New("unknown value-log GC manifest phase")
		}
		return manifest, nil
	default:
		return valueLogGCManifest{}, errors.New("unsupported value-log GC manifest version")
	}
}

func (db *DB) recoverValueLogGC() error {
	manifestPath := filepath.Join(db.opt.Dir, valueLogGCManifestName)
	if _, err := os.Stat(manifestPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	manifest, err := readValueLogGCManifest(db.opt.Dir)
	if err != nil {
		return err
	}
	if manifest.legacy {
		remaining, err := db.liveValuePointersLocked(manifest.SourceFileID)
		if err != nil {
			return err
		}
		if len(remaining) == 0 {
			if err := db.removeValueLogGCSource(manifest.SourceFileID); err != nil {
				return err
			}
		}
		return db.clearValueLogGCManifest()
	}

	switch manifest.Phase {
	case valueLogGCPrepared, valueLogGCPointersInstalled:
		remaining, err := db.liveValuePointersLocked(manifest.SourceFileID)
		if err != nil {
			return err
		}
		if len(remaining) != 0 {
			return db.finalizeValueLogGCManifest(manifest)
		}
		if err := db.validateValueLogGCReplacements(manifest); err != nil {
			return err
		}
		if err := db.removeValueLogGCSource(manifest.SourceFileID); err != nil {
			return err
		}
		manifest.Phase = valueLogGCSourceRemoved
		if err := db.writeValueLogGCManifest(manifest); err != nil {
			return err
		}
		return db.finalizeValueLogGCManifest(manifest)
	case valueLogGCSourceRemoved:
		if err := db.removeValueLogGCSource(manifest.SourceFileID); err != nil {
			return err
		}
		return db.finalizeValueLogGCManifest(manifest)
	case valueLogGCFinalized:
		return db.clearValueLogGCManifest()
	default:
		return errors.New("unknown value-log GC manifest phase")
	}
}

func (db *DB) validateValueLogGCReplacements(manifest valueLogGCManifest) error {
	for id := manifest.FirstReplacementFileID; ; id++ {
		if _, ok := db.valueLog.files[id]; !ok {
			return errors.New("value-log GC replacement segment missing")
		}
		if id == manifest.LastReplacementFileID {
			return nil
		}
	}
}

func (db *DB) removeValueLogGCSource(sourceFileID uint64) error {
	if file := db.valueLog.files[sourceFileID]; file != nil {
		if err := file.Close(); err != nil {
			return err
		}
		delete(db.valueLog.files, sourceFileID)
		delete(db.valueLog.sizes, sourceFileID)
		delete(db.valueLog.dirty, sourceFileID)
	}
	if err := os.Remove(db.valueLog.path(sourceFileID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDirectory(db.opt.Dir)
}

func (db *DB) finalizeValueLogGCManifest(manifest valueLogGCManifest) error {
	manifest.Phase = valueLogGCFinalized
	if err := db.writeValueLogGCManifest(manifest); err != nil {
		return err
	}
	return db.clearValueLogGCManifest()
}

func (db *DB) clearValueLogGCManifest() error {
	path := filepath.Join(db.opt.Dir, valueLogGCManifestName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return runMetadataOperation(db.opt, metadataStageManifestDeleteDirSync, func() error {
		return syncDirectory(db.opt.Dir)
	})
}
